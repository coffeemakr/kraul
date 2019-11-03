package main

import (
    "bytes"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "log"
    "net/http"
    "net/url"
    "os"
    "path"
    "regexp"
    "strings"
    "time"

    "golang.org/x/net/html"
)

func getAttribute(token html.Token, name string) (ok bool, value string) {
    // Iterate over all of the Token's attributes until we find an "href"
    for _, a := range token.Attr {
        if a.Key == name {
            value = a.Val
            ok = true
            break
        }
    }
    return
}

// Helper function to pull the href attribute from a Token
func getHref(token html.Token) (ok bool, href string) {
    return getAttribute(token, "href")
}

func resolveHref(base url.URL, href string) (*url.URL, error) {
    if base.Scheme == "" || base.Host == "" {
        return nil, errors.New("Base URL has no scheme / host")
    }
    url, err := url.Parse(href)
    if err != nil {
        return nil, err
    }
    // log.Println(href)
    switch url.Scheme {
    case "":
        url.Scheme = base.Scheme
    case "http", "https", "ftp":

    default:
        return url, nil
    }
    if url.Host == "" {
        url.Host = base.Host
    }
    if url.Path == "" {
        if base.Path == "" {
            url.Path = "/"
        } else {
            url.Path = base.Path
        }
    } else if !strings.HasPrefix(url.Path, "/") {
        baseBasePath, _ := path.Split(base.Path)
        url.Path = baseBasePath + url.Path
    }
    if err != nil {
        return nil, err
    }
    // log.Println(url.String())
    return url, nil
}

func getPhoneNumber(value string) (bool, string) {
    phoneNumberRegex := regexp.MustCompile(`^tel://(.+)$`)
    results := phoneNumberRegex.FindStringSubmatch(value)
    if results == nil {
        return false, ""
    }
    return true, results[0]
}

func extractLinks(documentUrl url.URL, r io.Reader) (links []url.URL, phoneNumbers []string) {
    z := html.NewTokenizer(r)
    var err error
    inHeader := false
    var baseURL = new(url.URL)
    *baseURL = documentUrl
    links = make([]url.URL, 0)
    phoneNumbers = make([]string, 0)
    for {
        tt := z.Next()
        switch {
        case tt == html.ErrorToken:
            // End of the document, we're done
            return
        case tt == html.StartTagToken:
            t := z.Token()

            switch t.Data {
            case "head":
                inHeader = true
            case "base":
                if inHeader {
                    ok, rawBaseUrl := getAttribute(t, "href")
                    if ok {
                        baseURL, err = url.Parse(rawBaseUrl)
                        if err != nil {
                            log.Println("Invalid base tag: ", err)
                            *baseURL = documentUrl
                        }
                    }
                }
            case "a":
                // Extract the href value, if there is one
                ok, rawUrl := getHref(t)
                if !ok {
                    continue
                }

                ok, phoneNumber := getPhoneNumber(rawUrl)
                if ok {
                    phoneNumbers = append(phoneNumbers, phoneNumber)
                    continue
                }

                url, err := resolveHref(*baseURL, rawUrl)
                if err != nil {
                    log.Println(err)
                    continue
                }
                // Make sure the url begines in http**
                url.Fragment = ""
                links = append(links, *url)
            }

        }
    }
}

type IndexedWebPage struct {
    URL          url.URL
    Content      string
    Links        []url.URL
    PhoneNumbers []string
}

type CrawlError struct {
    URL   url.URL
    Error error
}

type CrawJob struct {
    URL   url.URL
    Level int
}

// Extract all http** links from a given webpage
func loadPage(url url.URL) (result *IndexedWebPage, err error) {
    resp, err := http.Get(url.String())
    if err != nil {
        return
    }
    log.Printf("Loaded page %s\n", url.String())
    b := resp.Body
    defer b.Close() // close Body when the function returns
    var buf bytes.Buffer
    reader := io.TeeReader(b, &buf)
    links, phoneNumbers := extractLinks(url, reader)
    log.Printf("Extracted Links %s\n", url.String())
    result = &IndexedWebPage{
        URL:          url,
        Content:      buf.String(),
        Links:        links,
        PhoneNumbers: phoneNumbers,
    }
    return
}

func crawler(urlChannel chan url.URL, errorChannel chan CrawlError, crawlerId string, resultChannel chan IndexedWebPage) {
    for url := range urlChannel {
        log.Printf("[%s] Loading page %s\n", crawlerId, url.String())
        result, err := loadPage(url)
        log.Printf("Sending result... %s\n", url.String())
        if err != nil {
            errorChannel <- CrawlError{
                Error: err,
                URL:   url,
            }
        } else {
            resultChannel <- *result
        }
        time.Sleep(100 * time.Millisecond)
    }

}

func isWebUrl(url url.URL) bool {
    if url.Scheme == "http" || url.Scheme == "https" {
        return true
    }
    return false
}

func startCrawling(startUrl url.URL, resultChannel chan IndexedWebPage) {
    urlChannel := make(chan url.URL, 10000)
    errorChannel := make(chan CrawlError)
    intermediateResultChannel := make(chan IndexedWebPage)
    foundUrls := make(map[string]bool)
    for i := 0; i < 5; i++ {
        go crawler(urlChannel, errorChannel, fmt.Sprintf("Crawler %d", i), intermediateResultChannel)
    }

    var urlsCounter int = 1
    urlChannel <- startUrl
    for {
        select {
        case errorResult := <-errorChannel:
            urlsCounter--
            log.Printf("Error    %30s - %s\n", errorResult.URL.String(), errorResult.Error.Error())
        case result := <-intermediateResultChannel:
            urlsCounter--
            log.Printf("Spidered %30s - Links %d", result.URL.String(), len(result.Links))
            resultChannel <- result
            fmt.Print(".")
            for _, url := range result.Links {
                if isWebUrl(url) {
                    url.Fragment = ""
                    urlString := url.String()
                    if !foundUrls[urlString] {
                        urlChannel <- url
                        fmt.Print("<")
                        urlsCounter++
                        foundUrls[urlString] = true
                    }
                }
            }
            fmt.Print("\n")
        }
    }

}

func storeResult(result IndexedWebPage) error {
    body, err := json.Marshal(result)
    if err != nil {
        return err
    }
    req, err := http.NewRequest("PUT", "http://localhost:9200/text/article/"+url.PathEscape(result.URL.String()), bytes.NewReader(body))
    if err != nil {
        return err
    }

    req.Header.Set("Content-Type", "application/json")

    client := &http.Client{}
    callResult, err := client.Do(req)
    if err != nil {
        return err
    }
    log.Println(callResult)
    return nil
}

func main() {
    foundUrls := make(map[string]bool)

    // Channels
    chResponses := make(chan IndexedWebPage)

    url, err := url.Parse(os.Args[1])
    if err != nil {
        fmt.Println(err)
        return
    }

    go startCrawling(*url, chResponses)

    for result := range chResponses {
        log.Printf("\nResult %s", result.URL)
        if err := storeResult(result); err != nil {
            log.Fatalln(err)
        }
    }

    fmt.Println("\nFound", len(foundUrls), "unique urls:\n")

    for url := range foundUrls {
        fmt.Println(url)
    }
}
