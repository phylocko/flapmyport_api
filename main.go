package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
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
	// Settings
	defaultConfigFilename = "settings.conf"
	defaultListenAddress  = "0.0.0.0"
	defaultLogFilename    = "flapmyport_api.log"
	defaultListenPort     = 8080
	defaultDBHost         = "localhost"
	defaultDBUser         = "root"
	defaultDBName         = "snmpflapd"
	defaultDBPassword     = ""

	timeFormat      = "2006-01-02 15:04:05"
	flapChartWidth  = 333
	flapChartHeight = 10
	sqlRowsLimit    = 100000

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

type Config struct {
	LogFilename   string
	ListenAddress string
	ListenPort    int
	DBHost        string
	DBName        string
	DBUser        string
	DBPassword    string
}

func (c *Config) SqlDSN() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?parseTime=true",
		c.DBUser,
		c.DBPassword,
		c.DBHost,
		c.DBName,
	)
}

var ColorUp = color.RGBA{R: 10, G: 178, B: 38, A: 0xff}
var ColorUpState = color.RGBA{R: 125, G: 212, B: 139, A: 0xff}
var ColorDown = color.RGBA{R: 212, G: 57, B: 57, A: 0xff}
var ColorDownState = color.RGBA{R: 239, G: 106, B: 106, A: 0xff}
var ColorFlapping = color.RGBA{R: 255, G: 128, B: 0, A: 0xff}
var ColorUnknown = color.RGBA{R: 200, G: 200, B: 200, A: 0xff}

// DATA FORMATS

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

func (p *PortRow) CreateFlap() Flap {
	flap := Flap{
		Time: p.Time,
	}

	// Status stores as string in DB. Applying a workaround
	switch p.IfAdminStatus {
	case ifStatusUpString:
		flap.IfAdminStatus = ifStatusUP
	case ifStatusDownString:
		flap.IfAdminStatus = ifStatusDOWN
	}

	switch p.IfOperStatus {
	case ifStatusUpString:
		flap.IfOperStatus = ifStatusUP
	case ifStatusDownString:
		flap.IfOperStatus = ifStatusDOWN
	}

	return flap

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
		ORDER BY ipaddress, ifIndex, time ASC, timeticks ASC LIMIT %d;`,
		startTime.Format(timeFormat),
		endTime.Format(timeFormat),
		sqlRowsLimit,
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
	if host.Ipaddress != "" {
		result.Hosts = append(result.Hosts, *host)
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

func (f *Flapper) PortFlaps(startTime, endTime time.Time, ipAddress string, ifIndex int) []Flap {

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
		WHERE time >= '%s' AND time <= '%s' AND ipaddress = '%s' AND ifIndex = %d
		ORDER BY ipaddress, ifIndex, time ASC, timeticks ASC LIMIT 100;`,
		startTime.Format(timeFormat),
		endTime.Format(timeFormat),
		ipAddress,
		ifIndex,
	)

	var flaps []Flap
	for _, entry := range f.FetchFromDB(SQLQuery) {
		flaps = append(flaps, entry.CreateFlap())
	}

	return flaps
}

func (f *Flapper) FlapChart(q QueryParams) *FlapsDiagram {

	/*
		12:00			 13:00
		3600         1800          0 3600/333=10.81  1800/10.81 = 166
		 40          20/           0. 40/333=0.12012 20/0.1201 = 166
		 500	     250          0
		 [0 0 0 0 1 1 1 0 0 0 1 1 1]
		333          160          0.  500/333 = 1.5 250 / 1.5 = 166

		Cent := intervalSeconds / 333
		y := flapSecond / Cent

	*/

	intervalSeconds := q.End.Unix() - q.Start.Unix()
	cent := float64(intervalSeconds) / flapChartWidth

	timeLine := make([]int, flapChartWidth)

	EnumUnknown := 0
	EnumUp := 1
	EnumDown := 2
	EnumFlappingUp := 3
	EnumFlappingDown := 4

	flaps := f.PortFlaps(q.Start, q.End, q.Host, q.IfIndex)

	status := EnumUnknown

	for _, flap := range flaps {

		if status == EnumUnknown {
			if flap.IfOperStatus == ifStatusUP {
				status = EnumDown
			} else {
				status = EnumUp
			}
		}

		secondsFromStart := flap.Time.Unix() - q.Start.Unix()
		floatX := float64(secondsFromStart) / cent
		x := int(floatX)

		val := timeLine[x]
		if val == EnumUnknown {
			if flap.IfOperStatus == ifStatusUP {
				timeLine[x] = EnumUp
			} else {
				timeLine[x] = EnumDown
			}
		} else {
			if flap.IfOperStatus == ifStatusUP {
				timeLine[x] = EnumFlappingUp
			} else {
				timeLine[x] = EnumFlappingDown
			}

		}
	}

	// Fill timeline with colors
	colorLine := make([]color.RGBA, flapChartWidth)

	for i, enum := range timeLine {
		switch enum {
		case EnumUnknown:
			if status == EnumUp {
				colorLine[i] = ColorUpState
			} else if status == EnumDown {
				colorLine[i] = ColorDownState
			} else {
				colorLine[i] = ColorUnknown
			}

		case EnumUp:
			colorLine[i] = ColorUp
			status = EnumUp

		case EnumDown:
			colorLine[i] = ColorDown
			status = EnumDown

		case EnumFlappingUp:
			colorLine[i] = ColorFlapping
			status = EnumUp

		case EnumFlappingDown:
			colorLine[i] = ColorFlapping
			status = EnumDown

		}
	}

	flapsDiagram := CreateFlapsDiagram()

	for x, currentColor := range colorLine {
		flapsDiagram.drawCol(x, currentColor)
	}
	return flapsDiagram
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

	queryParams, err := s.ParseQueryParams(request)
	if err != nil {
		return
	}

	if queryParams.Host == "" {
		s.http404(response, "Host not given")
		return
	}
	if queryParams.IfIndex == 0 {
		s.http404(response, "Host not given")
		return
	}

	flapChart := s.flapper.FlapChart(queryParams)

	png.Encode(response, flapChart.img)
}

func (s *Server) ParseQueryParams(request *http.Request) (QueryParams, error) {

	queryParams := QueryParams{
		Start: time.Now().UTC().Add(-defaultReviewInterval),
		End:   time.Now().UTC(),
		Filter: Filter{
			PositiveConditions: []string{},
			NegativeConditions: []string{},
		},
	}

	query := request.URL.Query()

	if _, ok := query[actionCheck]; ok {
		queryParams.action = actionCheck
	}

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
			queryParams.End = time.Now().UTC()
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

func loadConfig() Config {
	var config = Config{
		LogFilename:   defaultLogFilename,
		ListenAddress: defaultListenAddress,
		ListenPort:    defaultListenPort,
		DBHost:        defaultDBHost,
		DBName:        defaultDBName,
		DBUser:        defaultDBUser,
		DBPassword:    defaultDBPassword,
	}

	_, err := toml.DecodeFile(defaultConfigFilename, &config)
	if err != nil {
		log.Fatal(err)
	}

	return config

}

// MAIN

func main() {

	config := loadConfig()
	flapper, err := createFlapper(config.SqlDSN())
	if err != nil {
		log.Fatal(err)
	}

	s := Server{flapper: flapper}

	http.HandleFunc("/", s.route)

	// Нормально ли, что здесь err криво наследует тип от err выше? Как пишут крутые чуваки?
	err = http.ListenAndServe(fmt.Sprintf("%s:%d", config.ListenAddress, config.ListenPort), nil)
	if err != nil {
		log.Fatal(err)
	}
}
