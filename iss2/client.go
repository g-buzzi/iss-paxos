package iss2

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strconv"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// Client overwrites read operation for Paxos
type Client struct {
	*paxi.HTTPClient
	serverIDs  []paxi.ID
	serverSize int
}

func NewClient(id paxi.ID) *Client {
	return &Client{
		HTTPClient: paxi.NewHTTPClient(id),
		serverIDs:  paxi.GetConfig().IDs(),
		serverSize: len(paxi.GetConfig().IDs()),
	}
}

// RESTGet issues a http call to node and return value and headers
func (c *Client) RESTGet(key paxi.Key) (paxi.Value, map[string]string, error) {
	return c.broadcast(key, nil)
}

// RESTPut puts new value as http.request body and return previous value
func (c *Client) RESTPut(key paxi.Key, value paxi.Value) (paxi.Value, map[string]string, error) {
	return c.broadcast(key, value)
}

func (c *Client) Get(key paxi.Key) (paxi.Value, error) {
	c.CID++
	v, _, err := c.RESTGet(key)
	return v, err
}

// Put puts new key value pair and return previous value (use REST)
// Default implementation of Client interface
func (c *Client) Put(key paxi.Key, value paxi.Value) error {
	c.CID++
	_, _, err := c.RESTPut(key, value)
	return err
}

func (c *Client) broadcast(key paxi.Key, value paxi.Value) (paxi.Value, map[string]string, error) {
	for _, serverId := range c.serverIDs {
		c.rest(serverId, key, value)
	}
	return nil, nil, nil
}

func (c *Client) rest(id paxi.ID, key paxi.Key, value paxi.Value) (paxi.Value, map[string]string, error) {
	// get url
	url := c.GetURL(id, key)

	method := http.MethodGet
	var body io.Reader
	if value != nil {
		method = http.MethodPut
		body = bytes.NewBuffer(value)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Error(err)
		return nil, nil, err
	}
	req.Header.Set(paxi.HTTPClientID, string(c.ID))
	req.Header.Set(paxi.HTTPCommandID, strconv.Itoa(c.CID))
	// r.Header.Set(HTTPTimestamp, strconv.FormatInt(time.Now().UnixNano(), 10))

	rep, err := c.Client.Do(req)
	if err != nil {
		log.Error(err)
		return nil, nil, err
	}
	defer rep.Body.Close()

	// get headers
	metadata := make(map[string]string)
	for k := range rep.Header {
		metadata[k] = rep.Header.Get(k)
	}

	if rep.StatusCode == http.StatusOK {
		b, err := ioutil.ReadAll(rep.Body)
		if err != nil {
			log.Error(err)
			return nil, metadata, err
		}
		if value == nil {
			log.Debugf("node=%v type=%s key=%v value=%x", id, method, key, paxi.Value(b))
		} else {
			log.Debugf("node=%v type=%s key=%v value=%x", id, method, key, value)
		}
		return paxi.Value(b), metadata, nil
	}

	// http call failed
	dump, _ := httputil.DumpResponse(rep, true)
	log.Debugf("%q", dump)
	return nil, metadata, errors.New(rep.Status)
}
