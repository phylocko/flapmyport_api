// Copyright 2022 Vladislav Pavkin

package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	_ "github.com/go-sql-driver/mysql"
)

// Settings
const (
	defaultConfigFilename = "settings.conf"
	defaultListenAddress  = "0.0.0.0"
	defaultLogFilename    = "flapmyport_api.log"
	defaultListenPort     = 8080
	defaultDBHost         = "localhost"
	defaultDBUser         = "root"
	defaultDBName         = "snmpflapd"
	defaultDBPassword     = ""
	timeFormat            = "2006-01-02 15:04:05"
	flapChartWidth        = 333
	flapChartHeight       = 10
	sqlRowsLimit          = 100000
	ifStatusUpCaption     = "up"
	ifStatusDownCaption   = "down"
	actionReview          = "review"
	actionFlapChart       = "flapchart"
	actionFlapHistory     = "flaphistory"
	actionCheck           = "check"
	defaultReviewInterval = time.Hour
	getParamIfIndex       = "ifindex"
	getParamHost          = "host"
	getParamStartTime     = "start"
	getParamEndTime       = "end"
	getParamInterval      = "interval"
	getParamFilter        = "filter"
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

var config = Config{
	LogFilename:   defaultLogFilename,
	ListenAddress: defaultListenAddress,
	ListenPort:    defaultListenPort,
	DBHost:        defaultDBHost,
	DBName:        defaultDBName,
	DBUser:        defaultDBUser,
	DBPassword:    defaultDBPassword,
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

var (
	// flags
	version            string
	build              string
	flagVerbose        bool
	flagConfigFilename string
	flagVersion        bool

	ColorUp        = color.RGBA{R: 10, G: 178, B: 38, A: 0xff}
	ColorUpState   = color.RGBA{R: 125, G: 212, B: 139, A: 0xff}
	ColorDown      = color.RGBA{R: 212, G: 57, B: 57, A: 0xff}
	ColorDownState = color.RGBA{R: 239, G: 106, B: 106, A: 0xff}
	ColorFlapping  = color.RGBA{R: 255, G: 128, B: 0, A: 0xff}
	ColorUnknown   = color.RGBA{R: 200, G: 200, B: 200, A: 0xff}
)

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
	Id           int
	Sid          string
	Time         time.Time
	TimeTicks    int64
	Ipaddress    string
	Hostname     *string
	IfIndex      int
	IfName       *string
	IfAlias      *string
	IfOperStatus string
}

func (p *PortRow) CreateFlap() Flap {
	return Flap{
		Time:         p.Time,
		IfOperStatus: p.IfOperStatus,
	}

}

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
	Time         time.Time
	IfOperStatus string
}

func (flap *Flap) FromDB(row PortRow) {
	flap.Time = row.Time
	flap.IfOperStatus = row.IfOperStatus
}

type PortView struct {
	IfIndex       int        `json:"ifIndex"`
	IfName        string     `json:"ifName"`
	IfAlias       string     `json:"ifAlias"`
	IfOperStatus  string     `json:"ifOperStatus"`
	FlapCount     int        `json:"flapCount"`
	FirstFlapTime *time.Time `json:"firstFlapTime"` // why?
	LastFlapTime  *time.Time `json:"lastFlapTime"`  // why?
	IsBlacklisted bool       `json:"isBlacklisted"`
}

func (p *PortView) FromDB(r PortRow) {
	p.IfIndex = r.IfIndex

	if r.IfName == nil {
		p.IfName = fmt.Sprintf("<ifIndex %d>", r.IfIndex)
	} else {
		p.IfName = *r.IfName
	}

	if r.IfAlias != nil {
		p.IfAlias = *r.IfAlias
	}

	p.FirstFlapTime = &r.Time
	p.LastFlapTime = &r.Time
	p.FlapCount = 1
	p.IfOperStatus = r.IfOperStatus
}

func (p *PortView) updateFromDB(r PortRow) {
	if p.IfIndex != r.IfIndex {
		panic("Wrong usage of PortView.UpdateFromDB")
	}
	p.FlapCount++
	if r.IfAlias != nil {
		p.IfAlias = *r.IfAlias
	}

	if r.Time.Before(*p.FirstFlapTime) {
		p.FirstFlapTime = &r.Time
	} else if r.Time.After(*p.LastFlapTime) {
		p.LastFlapTime = &r.Time
	}
}

type Host struct {
	Name      string     `json:"name"`
	Ipaddress string     `json:"ipaddress"`
	Ports     []PortView `json:"ports"`
}

func (h *Host) FromDB(r PortRow) {
	h.Ipaddress = r.Ipaddress
	h.Name = ""
	if r.Hostname != nil {
		h.Name = *r.Hostname
	}

	port := PortView{}
	port.FromDB(r)
	h.Ports = append(h.Ports, port)
}

func (h *Host) UpdateFromDB(r PortRow) {
	if h.Ipaddress != r.Ipaddress {
		panic("Wrong usage of Host.UpdateFromDB")
	}
	h.Name = ""
	if r.Hostname != nil {
		h.Name = *r.Hostname
	}

	// Decide if we need to update existing port of create new one
	for i, port := range h.Ports {
		if port.IfIndex == r.IfIndex {
			h.Ports[i].updateFromDB(r)
			return
		}
	}
	port := PortView{}
	port.FromDB(r)
	h.Ports = append(h.Ports, port)

}

// FLAPCHART

type FlapsDiagram struct {
	img *image.RGBA
}

func (f *FlapsDiagram) drawCol(x int, color color.RGBA) {
	for y := 0; y < flapChartHeight; y++ {
		f.img.Set(x, y, color)
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
	Conditions []string
}

func (f *Filter) ParseFilter(v url.Values) {
	filter, ok := v["filter"]
	if !ok {
		return
	}

	keywords := strings.Fields(filter[0])
	for _, kw := range keywords {
		if strings.HasPrefix(kw, "!") {
			if len(kw) < 2 {
				continue
			}
			kw = kw[1:]
			condition := fmt.Sprintf(`AND (hostname 
				NOT LIKE "%%%[1]s%%" AND ipaddress 
				NOT LIKE "%%%[1]s%%" AND ifAlias 
				NOT LIKE "%%%[1]s%%")`,
				kw,
			)
			f.Conditions = append(f.Conditions, condition)

		} else {
			condition := fmt.Sprintf(`AND (hostname 
			LIKE "%%%[1]s%%" OR ipaddress 
			LIKE "%%%[1]s%%" OR ifAlias 
			LIKE "%%%[1]s%%")`,
				kw,
			)
			f.Conditions = append(f.Conditions, condition)

		}

	}
}

func (f *Flapper) Review(startTime, endTime time.Time, filter Filter) (ReviewResult, error) {

	SQLQuery := fmt.Sprintf(`SELECT id,
 		sid, 
		CONVERT_TZ(time, @@session.time_zone, 'UTC'),
		timeticks,
		ipaddress, 
		hostname, 
		ifIndex, 
		ifName, 
		ifAlias, 
		ifOperStatus
		FROM ports 
		WHERE CONVERT_TZ(time, @@session.time_zone, 'UTC') >= '%s' 
		AND CONVERT_TZ(time, @@session.time_zone, 'UTC') <= '%s'
		AND ifName NOT LIKE '%%.%%'
		%s
		ORDER BY ipaddress, ifIndex, time ASC, timeticks ASC LIMIT %d;`,
		startTime.Format(timeFormat),
		endTime.Format(timeFormat),
		strings.Join(filter.Conditions, " "),
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
			host.FromDB(portRow)

		} else if host.Ipaddress == portRow.Ipaddress {
			host.UpdateFromDB(portRow)

		} else {
			result.Hosts = append(result.Hosts, *host)
			host = &Host{}
			host.FromDB(portRow)

		}

	}
	if host.Ipaddress != "" {
		result.Hosts = append(result.Hosts, *host)
	}
	return result, nil

}

func (f *Flapper) FetchFromDB(query string) []PortRow {
	var portRows []PortRow

	rows, err := f.db.Query(query)
	if err != nil {
		log.Printf("Unable to connect DB: %s", err)

	} else {
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
				&portRow.IfOperStatus,
			)
			if err != nil {
				log.Fatal(err)
			}
			portRows = append(portRows, portRow)

		}
	}
	return portRows
}

func (f *Flapper) PortFlaps(startTime, endTime time.Time, ipAddress string, ifIndex int) []Flap {

	SQLQuery := fmt.Sprintf(`SELECT id,
 		sid, 
		CONVERT_TZ(time, @@session.time_zone, 'UTC'),
		timeticks,
		ipaddress, 
		hostname, 
		ifIndex, 
		ifName, 
		ifAlias, 
		ifOperStatus
		FROM ports 
		WHERE CONVERT_TZ(time, @@session.time_zone, 'UTC') >= '%s' 
		AND CONVERT_TZ(time, @@session.time_zone, 'UTC') <= '%s' 
		AND ipaddress = '%s' AND ifIndex = %d
		AND ifName NOT LIKE '%%.%%'
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
	cent := float64(intervalSeconds) / (flapChartWidth - 1)

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
			if flap.IfOperStatus == ifStatusUpCaption {
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
			if flap.IfOperStatus == ifStatusUpCaption {
				timeLine[x] = EnumUp
			} else {
				timeLine[x] = EnumDown
			}
		} else {
			if flap.IfOperStatus == ifStatusUpCaption {
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

func (s Server) Index(response http.ResponseWriter) {
	message := "FlapMyPort API is ready"
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

	results, _ := s.flapper.Review(q.Start, q.End, q.Filter)

	jsonResults, err := json.Marshal(results)
	if err != nil {
		log.Printf("%s error: %s", request.URL, err)
		response.WriteHeader(http.StatusInternalServerError)
		return
	}
	response.Header().Add("Content-Type", "application/json")
	response.Write(jsonResults)

}

func (s *Server) HandleCheck(response http.ResponseWriter, request *http.Request) {
	logVerbose(fmt.Sprintln("?check requested"))

	result := CheckResult{CheckResult: "flapmyport"}

	jsonResult, err := json.Marshal(result)
	if err != nil {
		log.Printf("%s error: %s", request.URL, err)
		response.WriteHeader(http.StatusInternalServerError)
		return
	}
	response.Write(jsonResult)
}

func (s *Server) HandleFlapChart(response http.ResponseWriter, request *http.Request, q QueryParams) {

	queryParams, err := s.ParseQueryParams(request)
	if err != nil {
		log.Printf("%s error: %s", request.URL, err)
		return
	}

	if queryParams.Host == "" {
		msg := "Host not given"
		log.Printf("%s error: %s", request.URL, msg)
		s.http400(response, msg)
		return
	}
	if queryParams.IfIndex == 0 {
		msg := "Host not given"
		log.Printf("%s error: %s", request.URL, msg)
		s.http400(response, msg)
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
			Conditions: []string{},
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
		if startStr[0] != "" {
			if start, err := time.Parse(timeFormat, startStr[0]); err != nil {
				log.Printf("%s invalid start time: %s", request.URL, err)
				return queryParams, err
			} else {
				queryParams.Start = start
			}
		}
	}

	if endStr, ok := query[getParamEndTime]; ok {
		if endStr[0] != "" {
			if end, err := time.Parse(timeFormat, endStr[0]); err != nil {
				log.Printf("%s invalid end time: %s", request.URL, err)
				return queryParams, err
			} else {
				queryParams.End = end
			}
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
		log.Printf("%s ParseQueryParams error: %s", request.URL, err)
		s.http400(response, err.Error())
		return
	}

	logVerbose(fmt.Sprintf("/%s requested", queryParams.action))

	switch queryParams.action {

	case actionReview:
		s.HandleReview(response, request, queryParams)

	case actionFlapChart:
		s.HandleFlapChart(response, request, queryParams)

	case actionCheck:
		s.HandleCheck(response, request)

	default:
		s.Index(response)
	}

}

func readConfigFile(file *string) {
	if _, err := toml.DecodeFile(*file, &config); err != nil {
		msg := fmt.Sprintf("%s not found. Suppose we're using environment variables", *file)
		fmt.Println(msg)
		log.Println(msg)
	}
}

func readConfigEnv() {

	if logFilename, exists := os.LookupEnv("LOGFILE"); exists {
		config.LogFilename = logFilename
	}

	if listenAddress, exists := os.LookupEnv("LISTEN_ADDRESS"); exists {
		config.ListenAddress = listenAddress
	}

	if listenPort, exists := os.LookupEnv("LISTEN_PORT"); exists {
		if intPort, error := strconv.Atoi(listenPort); error != nil {
			msg := "Wrong environment variable LISTEN_PORT"
			fmt.Println(msg)
			log.Fatalln(msg)

		} else {
			config.ListenPort = intPort
		}

	}

	if dbHost, exists := os.LookupEnv("DBHOST"); exists {
		config.DBHost = dbHost
	}

	if dbName, exists := os.LookupEnv("DBNAME"); exists {
		config.DBName = dbName
	}

	if dbUser, exists := os.LookupEnv("DBUSER"); exists {
		config.DBUser = dbUser
	}

	if dbPassword, exists := os.LookupEnv("DBPASSWORD"); exists {
		config.DBPassword = dbPassword
	}
}

func logVerbose(s string) {
	if flagVerbose {
		log.Print(s)
	}
}

// MAIN

func init() {

	// Reading flags
	flag.BoolVar(&flagVersion, "V", false, "Print version information and quit")
	flag.StringVar(&flagConfigFilename, "f", defaultConfigFilename, "Location of config file")
	flag.BoolVar(&flagVerbose, "v", false, "Enable verbose logging")

	flag.Parse()

	if flagVersion {
		build := fmt.Sprintf("FlapMyPort snmpflapd version %s, build %s", version, build)
		fmt.Println(build)
		os.Exit(0)
	}

	// Reading config
	readConfigFile(&flagConfigFilename)
	readConfigEnv()

	logVerbose(fmt.Sprintf("DBHost: %s", config.DBHost))
	logVerbose(fmt.Sprintf("DBName: %s", config.DBName))
	logVerbose(fmt.Sprintf("DBUser: %s", config.DBUser))

}

func createServer(c Config) *Server {
	flapper, err := createFlapper(c.SqlDSN())
	if err != nil {
		log.Fatalf("Unable to create server: %s", err)
	}
	s := Server{flapper: flapper}
	return &s
}

func main() {

	s := createServer(config)

	fmt.Println("flapmyport_api version:", version, "build:", build)
	msg := fmt.Sprintf("Listening on %s:%d", config.ListenAddress, config.ListenPort)
	fmt.Println(msg)

	http.HandleFunc("/", s.route)

	listenSocket := fmt.Sprintf("%s:%d", config.ListenAddress, config.ListenPort)
	err := http.ListenAndServe(listenSocket, nil)
	if err != nil {
		log.Fatal(err)
	}
}
