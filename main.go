package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"image"
	"image/color"
	"image/png"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	timeFormat      = "2006-01-02 15:04:05"
	flapChartWidth  = 333
	flapChartHeight = 10

	ifStatusUP          = 1
	ifStatusDOWN        = 2
	ifStatusUpString    = "1"
	ifStatusDownString  = "2"
	ifStatusUpCaption   = "up"
	ifStatusDownCaption = "down"

	actionReview      = "review"
	actionFlapChart   = "flapchart"
	actionFlapHistory = "flaphistory"
	actionCheck       = "check"

	defaultReviewInterval = time.Hour

	getParamIfIndex   = "ifindex"
	getParamHost      = "host"
	getParamStartTime = "start"
	getParamEndTime   = "end"
	getParamInterval  = "interval"
	getParamFilter    = "filter"
)

// DATA FORMATS

type FMPTime time.Time

func (f FMPTime) MarshalJSON() ([]byte, error) {
	time := time.Time(f).Format(timeFormat)
	timeStr := fmt.Sprintf(time)
	return []byte(timeStr), nil
}

type CheckResult struct {
	CheckResult string `json:"checkResult"`
}

type QueryParams struct {
	action  string
	IfIndex int
	Host    string
	Start   time.Time
	End     time.Time
	Filter  Filter
}

// PortRow is a DB row representation
type PortRow struct {
	Id            int
	Sid           string
	Time          time.Time
	TimeTicks     int64
	Ipaddress     string
	Hostname      string
	IfIndex       int
	IfName        string
	IfAlias       string
	IfAdminStatus string
	IfOperStatus  string
}

// ReviewResult is a structure
type ReviewResult struct {
	Params Params `json:"params"`
	Hosts  []Host `json:"hosts"`
}

type Params struct {
	TimeStart     *time.Time `json:"timeStart"`
	TimeEnd       *time.Time `json:"timeEnd"`
	FirstFlapTime *time.Time `json:"firstFlapTime"`
	LastFlapTime  *time.Time `json:"lastFlapTime"`
	OldestFlapID  int        `json:"oldestFlapID"`
}

type Flap struct {
	Time          time.Time
	IfOperStatus  int
	IfAdminStatus int
}

func (f *Flap) CreateFromDB(r PortRow) {
	f.Time = r.Time

	// Status stores as string in DB. Applying a workaround
	if r.IfAdminStatus == ifStatusUpString {
		f.IfAdminStatus = ifStatusUP
	} else if r.IfAdminStatus == ifStatusDownString {
		f.IfAdminStatus = ifStatusDOWN
	}

	if r.IfOperStatus == ifStatusUpString {
		f.IfOperStatus = ifStatusUP
	} else if r.IfOperStatus == ifStatusDownString {
		f.IfOperStatus = ifStatusDOWN
	}
}

type Port struct {
	IfIndex       int        `json:"ifIndex"`
	IfName        string     `json:"ifName"`
	IfAlias       string     `json:"ifAlias"`
	IfOperStatus  string     `json:"ifOperStatus"`
	FlapCount     int        `json:"flapCount"`
	FirstFlapTime *time.Time `json:"firstFlapTime"` // why?
	LastFlapTime  *time.Time `json:"lastFlapTime"`  // why?
	IsBlacklisted bool       `json:"isBlacklisted"`
}

func (p *Port) CreateFromDB(r PortRow) {
	p.IfIndex = r.IfIndex
	p.IfName = r.IfName
	p.IfAlias = r.IfAlias
	p.FirstFlapTime = &r.Time
	p.LastFlapTime = &r.Time
	p.FlapCount = 1

	switch r.IfOperStatus {

	case ifStatusUpString:
		p.IfOperStatus = ifStatusUpCaption

	case ifStatusDownString:
		p.IfOperStatus = ifStatusDownCaption

	}

}

func (p *Port) updateFromDB(r PortRow) {
	if p.IfIndex != r.IfIndex {
		panic("Wrong usage of Port.UpdateFromDB")
	}
	p.FlapCount++
	p.IfAlias = r.IfAlias
	if r.Time.Before(*p.FirstFlapTime) {
		p.FirstFlapTime = &r.Time
	} else if r.Time.After(*p.LastFlapTime) {
		p.LastFlapTime = &r.Time
	}
}

type Host struct {
	Name      string `json:"name"`
	Ipaddress string `json:"ipaddress"`
	Ports     []Port `json:"ports"`
}

func (h *Host) CreateFromDB(r PortRow) {
	h.Ipaddress = r.Ipaddress
	h.Name = r.Hostname
	port := Port{}
	port.CreateFromDB(r)
	h.Ports = append(h.Ports, port)
}

func (h *Host) UpdateFromDB(r PortRow) {
	if h.Ipaddress != r.Ipaddress {
		panic("Wrong usage of Host.UpdateFromDB")
	}
	h.Name = r.Hostname

	// Decide if we need to update existing port of create new one
	for i, port := range h.Ports {
		if port.IfIndex == r.IfIndex {
			h.Ports[i].updateFromDB(r)
			return
		}
	}
	port := Port{}
	port.CreateFromDB(r)
	h.Ports = append(h.Ports, port)

}

// FLAPCHART

type FlapsDiagram struct {
	img *image.RGBA
}

func (f *FlapsDiagram) drawCol(x int, color color.RGBA) {
	for y := 0; y < flapChartHeight; y++ {
		f.img.Set(x, y, color)
		color.R -= 1
	}
}

func CreateFlapsDiagram() *FlapsDiagram {

	upLeft := image.Point{X: 0, Y: 0}
	lowRight := image.Point{X: flapChartWidth, Y: flapChartHeight}

	flapsDiagram := FlapsDiagram{
		img: image.NewRGBA(image.Rectangle{Min: upLeft, Max: lowRight}),
	}
	return &flapsDiagram
}

// FLAPPER

type Flapper struct {
	db *sql.DB
}

func createFlapper(dsn string) (*Flapper, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	f := &Flapper{db: db}
	return f, nil

}

type Filter struct {
	PositiveConditions []string
	NegativeConditions []string
}

func (f *Filter) ParseFilter(v url.Values) {
	filter, ok := v["filter"]
	if !ok {
		return
	}

	keywords := strings.Split(filter[0], "") // works incorrect :(
	for _, kw := range keywords {
		if strings.HasPrefix("!", kw) {
			kw := kw[1:]
			f.NegativeConditions = append(f.NegativeConditions, kw) // replace `!`
		} else {
			kw := strings.TrimPrefix(kw, "!")
			if len(kw) > 0 {
				f.PositiveConditions = append(f.PositiveConditions, kw)
			}
		}
	}
}

func (f *Flapper) Review(startTime, endTime time.Time) (ReviewResult, error) {

	/*
		SELECT hostname, ipaddress, ifName, ifAlias
		FROM ports

			AND
			(hostname LIKE "%1%" OR ipaddress LIKE "%1%" OR ifAlias LIKE "%1%")
			AND
			(hostname LIKE "%m9%" OR ipaddress LIKE "%m9%" OR ifAlias LIKE "%m9%")
			AND NOT
			(hostname LIKE "%r0%" OR ipaddress LIKE "%r0%" OR ifAlias LIKE "%r0%")

		GROUP BY hostname, ipaddress, ifName, ifAlias;
	*/

	SQLQuery := fmt.Sprintf(`SELECT id,
 		sid, 
		time,
		timeticks,
		ipaddress, 
		hostname, 
		ifIndex, 
		ifName, 
		ifAlias, 
 		ifAdminStatus, 
		ifOperStatus
		FROM ports 
		WHERE time >= '%s' AND time <= '%s'
		ORDER BY ipaddress, ifIndex, time ASC, timeticks ASC LIMIT 100;`,
		startTime.Format(timeFormat),
		endTime.Format(timeFormat),
	)

	result := ReviewResult{
		Hosts: make([]Host, 0, 100),
		Params: Params{
			TimeStart: &startTime,
			TimeEnd:   &endTime,
		},
	}

	host := &Host{}

	for _, portRow := range f.FetchFromDB(SQLQuery) {

		// 0 instead of nil if no flaps because clients crashed seeing null :)
		if result.Params.OldestFlapID == 0 {
			result.Params.OldestFlapID = portRow.Id
		}

		if result.Params.FirstFlapTime == nil {
			result.Params.FirstFlapTime = &portRow.Time
		}
		result.Params.LastFlapTime = &portRow.Time

		if host.Ipaddress == "" {
			host.CreateFromDB(portRow)

		} else if host.Ipaddress == portRow.Ipaddress {
			host.UpdateFromDB(portRow)

		} else {
			result.Hosts = append(result.Hosts, *host)
			host = &Host{}
			host.CreateFromDB(portRow)

		}

	}
	return result, nil

}

func (f *Flapper) FetchFromDB(query string) []PortRow {
	var portRows []PortRow

	rows, _ := f.db.Query(query)
	for rows.Next() {
		portRow := PortRow{}
		err := rows.Scan(
			&portRow.Id,
			&portRow.Sid,
			&portRow.Time,
			&portRow.TimeTicks,
			&portRow.Ipaddress,
			&portRow.Hostname,
			&portRow.IfIndex,
			&portRow.IfName,
			&portRow.IfAlias,
			&portRow.IfAdminStatus,
			&portRow.IfOperStatus,
		)
		if err != nil {
			log.Fatal(err)
		}
		portRows = append(portRows, portRow)
	}
	return portRows
}

func (f *Flapper) PortFlaps(startTime, endTime time.Time, ipAddress string, ifIndex time.Time) []Flap {
	var flaps []Flap

	SQLQuery := fmt.Sprintf(`SELECT FROM ports 
		WHERE ipaddress='%s' 
		AND ifindex=%s AND time >= '%s' 
		AND time <= '%s'
		ORDER BY time ASC, timeticks ASC;`,
		ipAddress,
		ifIndex,
		startTime,
		endTime,
	)

	entries := f.FetchFromDB(SQLQuery)
	for _, entry := range entries {

		flap := Flap{}
		flap.CreateFromDB(entry)
		flaps = append(flaps, flap)
	}

	return flaps
}

// SERVER

type Server struct {
	flapper *Flapper
}

func (s Server) http404(response http.ResponseWriter, message string) {
	if message == "" {
		message = "Resource not found"
	}
	response.WriteHeader(http.StatusNotFound)
	response.Write([]byte(message))
}

func (s Server) http400(response http.ResponseWriter, message string) {

	if message == "" {
		message = "Bad request"
	}

	response.WriteHeader(http.StatusBadRequest)
	response.Write([]byte(message))
}

func (s *Server) HandleReview(response http.ResponseWriter, request *http.Request, q QueryParams) {

	fmt.Println("request.URL.RawQuery: ", request.URL.RawQuery)

	results, err := s.flapper.Review(q.Start, q.End)

	jsonResults, err := json.Marshal(results)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		return
	}
	response.Write(jsonResults)

}

func (s *Server) HandleCheck(response http.ResponseWriter) {
	result := CheckResult{CheckResult: "flapmyport"}

	jsonResult, err := json.Marshal(result)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		return
	}
	response.Write(jsonResult)
}

func (s *Server) HandleFlapChart(response http.ResponseWriter, request *http.Request, q QueryParams) {

	//queryData := request.URL.Query()

	//host, ok := queryData["host"]
	//if !ok {
	//	s.http400(response, "Parameter `host` is missing")
	//	return
	//}

	//ifIndex, ok := queryData[getParamIfIndex]
	//if !ok {
	//	s.http400(response, fmt.Sprintf("Parameter `%s` is missing", getParamIfIndex))
	//	return
	//}
	//
	////fmt.Println(host[0], ifIndex[0])

	flapChart := CreateFlapsDiagram()
	ColorUp := color.RGBA{R: 205, G: 240, B: 185, A: 0xff}
	ColorDown := color.RGBA{R: 205, G: 51, B: 51, A: 0xff}
	ColorFlapping := color.RGBA{R: 255, G: 128, B: 0, A: 0xff}
	ColorUnknown := color.RGBA{R: 200, G: 200, B: 200, A: 0xff}

	for x := 0; x < flapChartWidth; x++ {
		currentColor := ColorDown
		if x < 25 {
			currentColor = ColorUnknown
		} else if x < 240 {
			currentColor = ColorUp
		} else if x < 300 {
			currentColor = ColorFlapping
		} else {
			currentColor = ColorUp
		}
		flapChart.drawCol(x, currentColor)
	}
	png.Encode(response, flapChart.img)
}

func (s *Server) ParseQueryParams(request *http.Request) (QueryParams, error) {

	queryParams := QueryParams{
		Start: time.Now().Add(-defaultReviewInterval),
		End:   time.Now(),
		Filter: Filter{
			PositiveConditions: []string{},
			NegativeConditions: []string{},
		},
	}

	query := request.URL.Query()

	if _, ok := query[actionReview]; ok {
		queryParams.action = actionReview
	}

	if _, ok := query[actionFlapHistory]; ok {
		queryParams.action = actionFlapHistory
	}

	if _, ok := query[actionFlapChart]; ok {
		queryParams.action = actionFlapChart
	}

	if ifIndexStr, ok := query[getParamIfIndex]; ok {
		queryParams.IfIndex, _ = strconv.Atoi(ifIndexStr[0])
	}

	if host, ok := query[getParamHost]; ok {
		queryParams.Host = host[0]
	}

	if startStr, ok := query[getParamStartTime]; ok {
		if start, err := time.Parse(timeFormat, startStr[0]); err != nil {
			return queryParams, err

		} else {
			queryParams.Start = start
		}
	}

	if endStr, ok := query[getParamEndTime]; ok {
		if end, err := time.Parse(timeFormat, endStr[0]); err != nil {
			return queryParams, err

		} else {
			queryParams.End = end
		}
	}

	// `start` and `end` are overwritten if `interval` is provided
	if intervalStr, ok := query[getParamInterval]; ok {
		interval, err := strconv.Atoi(intervalStr[0])
		if err == nil {
			duration := time.Duration(interval) * time.Second
			queryParams.End = time.Now()
			queryParams.Start = queryParams.End.Add(-duration)
		}
	}

	queryParams.Filter.ParseFilter(request.URL.Query())

	return queryParams, nil
}

func (s *Server) route(response http.ResponseWriter, request *http.Request) {

	queryParams, err := s.ParseQueryParams(request)
	if err != nil {
		s.http404(response, err.Error())
	}

	switch queryParams.action {

	case actionReview:
		s.HandleReview(response, request, queryParams)

	case actionFlapChart:
		s.HandleFlapChart(response, request, queryParams)

	case actionCheck:
		s.HandleCheck(response)

	default:
		s.http404(response, "Unknown action")
	}

}

// MAIN

func main() {
	flapper, err := createFlapper("developer:0o9i8u@tcp(localhost)/traps?parseTime=true")
	if err != nil {
		log.Fatal(err)
	}

	s := Server{flapper: flapper}

	http.HandleFunc("/", s.route)
	http.ListenAndServe(":8080", nil)
}
