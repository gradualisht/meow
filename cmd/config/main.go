package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/patrickbucher/meow"
	"github.com/valkey-io/valkey-go"
)

func main() {
	addr := flag.String("addr", "0.0.0.0", "listen to address")
	port := flag.Uint("port", 8000, "listen on port")
	flag.Parse()

	log.SetOutput(os.Stderr)

	// Initialize Valkey client
	valkeyURL := os.Getenv("VALKEY_URL")
	if valkeyURL == "" {
		log.Fatal("VALKEY_URL environment variable is not set")
	}

	parsedURL, err := url.Parse(valkeyURL)
	if err != nil {
		log.Fatalf("failed to parse VALKEY_URL: %v", err)
	}

	// Extract host and port
	hostPort := parsedURL.Host
	if hostPort == "" {
		log.Fatal("VALKEY_URL does not contain a valid host")
	}

	// Extract database number from path (e.g., /17)
	dbNumber := 0
	if parsedURL.Path != "" && parsedURL.Path != "/" {
		dbStr := strings.TrimPrefix(parsedURL.Path, "/")
		dbNumber, err = strconv.Atoi(dbStr)
		if err != nil {
			log.Fatalf("failed to parse database number from path '%s': %v", parsedURL.Path, err)
		}
	}

	options := valkey.ClientOption{
		InitAddress: []string{hostPort},
		SelectDB:    dbNumber,
	}

	valkeyClient, err := valkey.NewClient(options)
	if err != nil {
		log.Fatalf("failed to initialize Valkey client: %v", err)
	}

	http.HandleFunc("/endpoints/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getEndpoint(w, r, valkeyClient)
		case http.MethodPost:
			postEndpoint(w, r, valkeyClient)
		// TODO: support http.MethodDelete to delete endpoints
		default:
			log.Printf("request from %s rejected: method %s not allowed",
				r.RemoteAddr, r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	http.HandleFunc("/endpoints", func(w http.ResponseWriter, r *http.Request) {
		getEndpoints(w, r, valkeyClient)
	})

	listenTo := fmt.Sprintf("%s:%d", *addr, *port)
	log.Printf("listen to %s", listenTo)
	http.ListenAndServe(listenTo, nil)
}

func getEndpoint(w http.ResponseWriter, r *http.Request, vk valkey.Client) {
	log.Printf("GET %s from %s", r.URL, r.RemoteAddr)
	identifier, err := extractEndpointIdentifier(r.URL.String())
	if err != nil {
		log.Printf("extract endpoint identifier of %s: %v", r.URL, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	key := fmt.Sprintf("endpoints:%s", identifier)

	// Retrieve hash from Valkey
	hgetallCmd := vk.B().Hgetall().Key(key).Build()
	hashData, err := vk.Do(ctx, hgetallCmd).AsStrMap()
	if err != nil {
		log.Printf("failed to retrieve hash for key %s: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Check if key exists (empty hash means key doesn't exist)
	if len(hashData) == 0 {
		log.Printf(`no such endpoint "%s"`, identifier)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Parse status_online
	statusOnline, err := strconv.Atoi(hashData["status_online"])
	if err != nil {
		log.Printf("failed to parse status_online for key %s: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Parse fail_after
	failAfter, err := strconv.Atoi(hashData["fail_after"])
	if err != nil {
		log.Printf("failed to parse fail_after for key %s: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	payload := meow.EndpointPayload{
		Identifier:   hashData["identifier"],
		URL:          hashData["url"],
		Method:       hashData["method"],
		StatusOnline: uint16(statusOnline),
		Frequency:    hashData["frequency"],
		FailAfter:    uint8(failAfter),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("serialize payload: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

func postEndpoint(w http.ResponseWriter, r *http.Request, vk valkey.Client) {
	log.Printf("POST %s from %s", r.URL, r.RemoteAddr)
	buf := bytes.NewBufferString("")
	io.Copy(buf, r.Body)
	defer r.Body.Close()
	endpoint, err := meow.EndpointFromJSON(buf.String())
	if err != nil {
		log.Printf("parse JSON body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	key := fmt.Sprintf("endpoints:%s", endpoint.Identifier)

	// Check if endpoint already exists
	hgetallCmd := vk.B().Hgetall().Key(key).Build()
	hashData, err := vk.Do(ctx, hgetallCmd).AsStrMap()
	if err != nil {
		log.Printf("failed to check if endpoint exists: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	exists := len(hashData) > 0
	var status int
	if exists {
		// updating existing endpoint
		identifierPathParam, err := extractEndpointIdentifier(r.URL.String())
		if err != nil {
			log.Printf("extract endpoint identifier of %s: %v", r.URL, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if identifierPathParam != endpoint.Identifier {
			log.Printf("identifier mismatch: (ressource: %s, body: %s)",
				identifierPathParam, endpoint.Identifier)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		status = http.StatusNoContent
	} else {
		status = http.StatusCreated
	}

	// Store endpoint in Valkey
	hsetCmd := vk.B().Hset().Key(key).
		FieldValue().
		FieldValue("identifier", endpoint.Identifier).
		FieldValue("url", endpoint.URL.String()).
		FieldValue("method", endpoint.Method).
		FieldValue("status_online", strconv.Itoa(int(endpoint.StatusOnline))).
		FieldValue("frequency", endpoint.Frequency.String()).
		FieldValue("fail_after", strconv.Itoa(int(endpoint.FailAfter))).
		Build()

	if err := vk.Do(ctx, hsetCmd).Error(); err != nil {
		log.Printf("failed to store endpoint in Valkey: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(status)
}

func getEndpoints(w http.ResponseWriter, r *http.Request, vk valkey.Client) {
	if r.Method != http.MethodGet {
		log.Printf("request from %s rejected: method %s not allowed",
			r.RemoteAddr, r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	log.Printf("GET %s from %s", r.URL, r.RemoteAddr)
	ctx := context.Background()

	// Get all endpoint keys from Valkey
	keysCmd := vk.B().Keys().Pattern("endpoints:*").Build()
	keysResp, err := vk.Do(ctx, keysCmd).AsStrSlice()
	if err != nil {
		log.Printf("failed to retrieve keys from Valkey: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	payloads := make([]meow.EndpointPayload, 0)
	for _, key := range keysResp {
		// Get hash for each endpoint
		hgetallCmd := vk.B().Hgetall().Key(key).Build()
		hashData, err := vk.Do(ctx, hgetallCmd).AsStrMap()
		if err != nil {
			log.Printf("failed to retrieve hash for key %s: %v", key, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Parse status_online
		statusOnline, err := strconv.Atoi(hashData["status_online"])
		if err != nil {
			log.Printf("failed to parse status_online for key %s: %v", key, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Parse fail_after
		failAfter, err := strconv.Atoi(hashData["fail_after"])
		if err != nil {
			log.Printf("failed to parse fail_after for key %s: %v", key, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		payload := meow.EndpointPayload{
			Identifier:   hashData["identifier"],
			URL:          hashData["url"],
			Method:       hashData["method"],
			StatusOnline: uint16(statusOnline),
			Frequency:    hashData["frequency"],
			FailAfter:    uint8(failAfter),
		}
		payloads = append(payloads, payload)
	}

	data, err := json.Marshal(payloads)
	if err != nil {
		log.Printf("serialize payloads: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

const endpointIdentifierPatternRaw = "^/endpoints/([a-z][-a-z0-9]+)$"

var endpointIdentifierPattern = regexp.MustCompile(endpointIdentifierPatternRaw)

func extractEndpointIdentifier(endpoint string) (string, error) {
	matches := endpointIdentifierPattern.FindStringSubmatch(endpoint)
	if len(matches) == 0 {
		return "", fmt.Errorf(`endpoint "%s" does not match pattern "%s"`,
			endpoint, endpointIdentifierPatternRaw)
	}
	return matches[1], nil
}
