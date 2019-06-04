package relay

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/lafikl/consistent"
)

type Host struct {
	Name string
	Load int64
}

type Consistent struct {
	hosts     map[uint64]string
	sortedSet []uint64
	loadMap   map[string]*Host
	totalLoad int64

	sync.RWMutex
}

var bufPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}
var ErrBufferFull = errors.New("retry buffer full")

// HTTP is a relay for HTTP influxdb writes
type HTTP struct {
	addr   string
	name   string
	schema string

	cert     string
	rp       string
	ct       *consistent.Consistent
	closing  int64
	l        net.Listener
	backends []*httpBackend
}

type simplePoster struct {
	client *http.Client
	//location string
}

const (
	DefaultHTTPTimeout      = 10 * time.Second
	DefaultMaxDelayInterval = 10 * time.Second
	DefaultBatchSizeKB      = 512

	KB = 1024
	MB = 1024 * KB
)

type httpBackend struct {
	poster
	name     string
	location string
}

type poster interface {
	post([]byte, string, string, string) (*responseData, error)
	get([]byte, string, string, string) (*responseData, error)
}

type responseData struct {
	ContentType     string
	ContentEncoding string
	StatusCode      int
	Body            []byte
}

func NewHTTP(cfg HTTPConfig) (Relay, error) {
	h := new(HTTP)
	h.ct = consistent.New()

	h.addr = cfg.Addr
	h.name = cfg.Name

	h.cert = cfg.SSLCombinedPem
	h.rp = cfg.DefaultRetentionPolicy

	h.schema = "http"
	if h.cert != "" {
		h.schema = "https"
	}

	for i := range cfg.Outputs {
		backend, err := newHTTPBackend(&cfg.Outputs[i])
		if err != nil {
			return nil, err
		}
		// fmt.Errorf()
		h.backends = append(h.backends, backend)
		fmt.Println(backend.name)
		h.ct.Add(backend.name)
	}

	return h, nil
}

func newHTTPBackend(cfg *HTTPOutputConfig) (*httpBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	timeout := DefaultHTTPTimeout
	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing HTTP timeout '%v'", err)
		}
		timeout = t
	}

	var p poster = newSimplePoster(timeout, cfg.SkipTLSVerification)

	// If configured, create a retryBuffer per backend.
	// This way we serialize retries against each backend.
	if cfg.BufferSizeMB > 0 {
		max := DefaultMaxDelayInterval
		if cfg.MaxDelayInterval != "" {
			m, err := time.ParseDuration(cfg.MaxDelayInterval)
			if err != nil {
				return nil, fmt.Errorf("error parsing max retry time %v", err)
			}
			max = m
		}

		batch := DefaultBatchSizeKB * KB
		if cfg.MaxBatchKB > 0 {
			batch = cfg.MaxBatchKB * KB
		}

		p = newRetryBuffer(cfg.BufferSizeMB*MB, batch, max, p)
	}

	return &httpBackend{
		poster:   p,
		name:     cfg.Name,
		location: cfg.Location,
	}, nil
}

func newSimplePoster(timeout time.Duration, skipTLSVerification bool) *simplePoster {
	// Configure custom transport for http.Client
	// Used for support skip-tls-verification option
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: skipTLSVerification,
		},
	}

	return &simplePoster{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		// location: location,
	}
}

func (b *simplePoster) post(buf []byte, query string, auth string, location string) (*responseData, error) {
	req, err := http.NewRequest("POST", location, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.URL.RawQuery = query
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	fmt.Println(req)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err = resp.Body.Close(); err != nil {
		return nil, err
	}

	return &responseData{
		ContentType:     resp.Header.Get("Content-Type"),
		ContentEncoding: resp.Header.Get("Content-Encoding"),
		StatusCode:      resp.StatusCode,
		Body:            data,
	}, nil
}

func (b *simplePoster) get(buf []byte, query string, auth string, location string) (*responseData, error) {
	// values := url.Values{
	// 	"db": {"example"},
	// 	"q":  {"SELECT%20*%20FROM%20cpu_load_short"},
	// }
	// var p = url.Values{}
	// p.Add("db", "example")
	// p.Add("q", "SELECT * FROM cpu_load_short")
	// HttpGet get = new HttpGet(URLEncoder.encode(url,"UTF-8"));
	// encodeurl := url.QueryEscape(location + "?db=example&q=SELECT%20*%20FROM%20cpu_load_short")
	req, err := http.NewRequest("GET", location+"?db=example&q=SELECT%20*%20FROM%20cpu_load_short", nil)
	// req, err := http.NewRequest("GET", location+"?q='SELECT * FROM cpu_load_short'", nil)
	//req, err := http.NewRequest("GET", location, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	// req.URL.RawQuery = query
	// req.Header.Set("Content-Type", "application/json")
	// req.Body.
	// req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	fmt.Println(req)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var str string = string(data[:])
	fmt.Println(str)
	if err = resp.Body.Close(); err != nil {
		return nil, err
	}
	return &responseData{
		ContentType:     resp.Header.Get("Content-Type"),
		ContentEncoding: resp.Header.Get("Content-Encoding"),
		StatusCode:      resp.StatusCode,
		Body:            data,
	}, nil
}

func (h *HTTP) Stop() error {
	atomic.StoreInt64(&h.closing, 1)
	return h.l.Close()
}

func (h *HTTP) Name() string {
	if h.name == "" {
		return fmt.Sprintf("%s://%s", h.schema, h.addr)
	}
	return h.name
}

func (h *HTTP) Run() error {
	l, err := net.Listen("tcp", h.addr)
	if err != nil {
		return err
	}

	// support HTTPS
	if h.cert != "" {
		cert, err := tls.LoadX509KeyPair(h.cert, h.cert)
		if err != nil {
			return err
		}

		l = tls.NewListener(l, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}

	h.l = l

	log.Printf("Starting %s relay %q on %v", strings.ToUpper(h.schema), h.Name(), h.addr)

	err = http.Serve(l, h)
	if atomic.LoadInt64(&h.closing) != 0 {
		return nil
	}
	return err
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.URL.Path == "/ping" && (r.Method == "GET" || r.Method == "HEAD") {
		w.Header().Add("X-InfluxDB-Version", "relay")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// if r.URL.Path != "/write" {
	// 	jsonError(w, http.StatusNotFound, "invalid write endpoint")
	// 	return
	// }

	if r.Method == "POST" {
		queryParams := r.URL.Query()

		// fail early if we're missing the database
		if queryParams.Get("db") == "" {
			jsonError(w, http.StatusBadRequest, "missing parameter: db")
			return
		}

		if queryParams.Get("rp") == "" && h.rp != "" {
			queryParams.Set("rp", h.rp)
		}

		var body = r.Body

		if r.Header.Get("Content-Encoding") == "gzip" {
			b, err := gzip.NewReader(r.Body)
			if err != nil {
				jsonError(w, http.StatusBadRequest, "unable to decode gzip body")
			}
			defer b.Close()
			body = b
		}

		bodyBuf := getBuf()
		_, err := bodyBuf.ReadFrom(body)
		if err != nil {
			putBuf(bodyBuf)
			jsonError(w, http.StatusInternalServerError, "problem reading request body")
			return
		}

		precision := queryParams.Get("precision")
		points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
		if err != nil {
			putBuf(bodyBuf)
			jsonError(w, http.StatusBadRequest, "unable to parse points")
			return
		}

		outBuf := getBuf()
		for _, p := range points {
			if _, err = outBuf.WriteString(p.PrecisionString(precision)); err != nil {
				break
			}
			if err = outBuf.WriteByte('\n'); err != nil {
				break
			}
		}

		// done with the input points
		putBuf(bodyBuf)

		if err != nil {
			putBuf(outBuf)
			jsonError(w, http.StatusInternalServerError, "problem writing points")
			return
		}

		// normalize query string
		query := queryParams.Encode()
		outBytes := outBuf.Bytes()

		// check for authorization performed via the header
		authHeader := r.Header.Get("Authorization")

		var wg sync.WaitGroup
		wg.Add(len(h.backends))

		var responses = make(chan *responseData, len(h.backends))

		for _, b := range h.backends {
			// host, err := c.Get("cpu_usage")
			// if err != nil {
			// 	log.Fatal(err)
			// }
			// fmt.Println(host)

			// host1, err1 := c.Get("mem_usage")
			// if err1 != nil {
			// 	log.Fatal(err1)
			// }
			// fmt.Println(host1)
			var db_name = strings.Split(query, "=")[1]
			dest_host, err := h.ct.Get(db_name)
			if err != nil {
				return
			}
			b := b
			if b.name != dest_host {
				return
			} else {
				fmt.Println("ss")
			}

			// b.poster.data.location = b.poster.data.location
			// b.poster.location = b.poster.location + r.URL.Path
			// if r.Method != "POST" {
			// 	w.Header().Set("Allow", "POST")
			// 	if r.Method == "OPTIONS" {
			// 		w.WriteHeader(http.StatusNoContent)
			// 	} else {
			// 		jsonError(w, http.StatusMethodNotAllowed, "invalid write method")
			// 	}
			// 	return
			// }
			var location string
			if r.URL.Path == "/write" || r.URL.Path == "/query" {
				location = b.location + r.URL.Path
			} else {
				return
			}

			go func() {
				defer wg.Done()
				// var resp *responseData
				resp, err := b.post(outBytes, query, authHeader, location)
				fmt.Println(outBytes)
				fmt.Println(query)
				fmt.Println(authHeader)
				// fmt.Println(resp)

				if err != nil {
					log.Printf("Problem posting to relay %q backend %q: %v", h.Name(), b.name, err)
				} else {
					if resp.StatusCode/100 == 5 {
						log.Printf("5xx response for relay %q backend %q: %v", h.Name(), b.name, resp.StatusCode)
					}

					responses <- resp
				}
			}()
		}
		go func() {
			wg.Wait()
			close(responses)
			putBuf(outBuf)
		}()

		var errResponse *responseData

		for resp := range responses {
			switch resp.StatusCode / 100 {
			case 2:
				w.WriteHeader(http.StatusNoContent)
				return

			case 4:
				// user error
				resp.Write(w)
				return

			default:
				// hold on to one of the responses to return back to the client
				errResponse = resp
			}
		}

		// no successful writes
		if errResponse == nil {
			// failed to make any valid request...
			jsonError(w, http.StatusServiceUnavailable, "unable to write points")
			return
		}

		errResponse.Write(w)
	} else if r.Method == "GET" {
		// queryParams := r.URL.Query()
		// bd, _ := ioutil.ReadAll(r.Body)
		// fmt.Fprintf(w, "%s", bd)
		// r.ParseForm()

		fmt.Println(r.FormValue("q"))
		// fmt.Println(r.FormValue("db"))
		r.ParseMultipartForm(1024)
		fmt.Println(r.Form.Get("db"))
		dbName := r.Form.Get("db")
		q := r.Form.Get("q")
		// queryParams := r.URL.Query()
		// fail early if we're missing the database
		if dbName == "" {
			jsonError(w, http.StatusBadRequest, "missing parameter: db")
			return
		}

		if q == "" {
			jsonError(w, http.StatusBadRequest, "missing parameter: db")
			return
		}

		// if queryParams.Get("rp") == "" && h.rp != "" {
		// 	queryParams.Set("rp", h.rp)
		// }

		// var body = r.Body

		if r.Header.Get("Content-Encoding") == "gzip" {
			b, err := gzip.NewReader(r.Body)
			if err != nil {
				jsonError(w, http.StatusBadRequest, "unable to decode gzip body")
			}
			defer b.Close()
			// body = b
		}
		// fmt.Println(r.GetBody())
		// bodyBuf := getBuf()
		// _, err := bodyBuf.ReadFrom(body)
		// if err != nil {
		// 	putBuf(bodyBuf)
		// 	jsonError(w, http.StatusInternalServerError, "problem reading request body")
		// 	return
		// }
		// // outBytes1 := bodyBuf.Bytes()
		// fmt.Println(bodyBuf.String())

		// precision := queryParams.Get("precision")
		// fmt.Println(precision)
		// points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
		// if err != nil {
		// 	putBuf(bodyBuf)
		// 	jsonError(w, http.StatusBadRequest, "unable to parse points")
		// 	return
		// }

		outBuf := getBuf()
		// for _, p := range points {
		// 	if _, err = outBuf.WriteString(p.PrecisionString(precision)); err != nil {
		// 		break
		// 	}
		// 	if err = outBuf.WriteByte('\n'); err != nil {
		// 		break
		// 	}
		// }
		// json.Marshal(cmd)
		// test_body := bytes.NewBuffer(b)
		// outBuf.WriteString()

		// done with the input points
		// putBuf(bodyBuf)

		// if err != nil {
		// 	putBuf(outBuf)
		// 	jsonError(w, http.StatusInternalServerError, "problem writing points")
		// 	return
		// }

		// normalize query string
		// query := queryParams.Encode()
		outBytes := outBuf.Bytes()

		// check for authorization performed via the header
		authHeader := r.Header.Get("Authorization")

		var wg sync.WaitGroup
		wg.Add(len(h.backends))

		var responses = make(chan *responseData, len(h.backends))

		for _, b := range h.backends {
			// host, err := c.Get("cpu_usage")
			// if err != nil {
			// 	log.Fatal(err)
			// }
			// fmt.Println(host)

			// host1, err1 := c.Get("mem_usage")
			// if err1 != nil {
			// 	log.Fatal(err1)
			// }
			// fmt.Println(host1)
			// var db_name = strings.Split(q, "=")[1]
			destHost, err := h.ct.Get(dbName)
			if err != nil {
				return
			}
			b := b
			if b.name != destHost {
				return
			} else {
				fmt.Println("ss")
			}

			// b.poster.data.location = b.poster.data.location
			// b.poster.location = b.poster.location + r.URL.Path
			// if r.Method != "POST" {
			// 	w.Header().Set("Allow", "POST")
			// 	if r.Method == "OPTIONS" {
			// 		w.WriteHeader(http.StatusNoContent)
			// 	} else {
			// 		jsonError(w, http.StatusMethodNotAllowed, "invalid write method")
			// 	}
			// 	return
			// }
			var location string
			if r.URL.Path == "/write" || r.URL.Path == "/query" {
				location = b.location + r.URL.Path
			} else {
				return
			}

			go func() {
				defer wg.Done()
				// var resp *responseData
				resp, err := b.get(outBytes, q, authHeader, location)
				fmt.Println(outBytes)
				fmt.Println(q)
				fmt.Println(authHeader)
				// fmt.Println(resp)

				if err != nil {
					log.Printf("Problem posting to relay %q backend %q: %v", h.Name(), b.name, err)
				} else {
					if resp.StatusCode/100 == 5 {
						log.Printf("5xx response for relay %q backend %q: %v", h.Name(), b.name, resp.StatusCode)
					}

					responses <- resp
				}
			}()
		}
		go func() {
			wg.Wait()
			close(responses)
			putBuf(outBuf)
		}()

		var errResponse *responseData

		for resp := range responses {
			switch resp.StatusCode / 100 {
			case 2:
				// w.WriteHeader(resp.Body)
				resp.Write(w)
				return

			case 4:
				// user error
				resp.Write(w)
				return

			default:
				// hold on to one of the responses to return back to the client
				errResponse = resp
			}
		}

		// no successful writes
		if errResponse == nil {
			// failed to make any valid request...
			jsonError(w, http.StatusServiceUnavailable, "unable to write points")
			return
		}

		errResponse.Write(w)
	} else {
		fmt.Println("Method is not allowed")
		var errResponse *responseData
		w.WriteHeader(http.StatusNoContent)
		errResponse.Write(w)
	}
	// 	w.Header().Set("Allow", "POST")
	// 	if r.Method == "OPTIONS" {
	// 		w.WriteHeader(http.StatusNoContent)
	// 	} else {
	// 		jsonError(w, http.StatusMethodNotAllowed, "invalid write method")
	// 	}
	// 	return
	// }

}

func jsonError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	data := fmt.Sprintf("{\"error\":%q}\n", message)
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	w.WriteHeader(code)
	w.Write([]byte(data))
}

func (rd *responseData) Write(w http.ResponseWriter) {
	if rd.ContentType != "" {
		w.Header().Set("Content-Type", rd.ContentType)
	}

	if rd.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", rd.ContentEncoding)
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(rd.Body)))
	w.WriteHeader(rd.StatusCode)
	w.Write(rd.Body)
}

func putBuf(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}

func getBuf() *bytes.Buffer {
	if bb, ok := bufPool.Get().(*bytes.Buffer); ok {
		return bb
	}
	return new(bytes.Buffer)
}
