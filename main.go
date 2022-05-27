package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net/http"
	"strings"
	"time"
)

const timeFormat = "2006-01-02 15:04:05"
const noIndex = -1

// DATA FORMATS

type PortDBEntry struct {
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

type ReviewResult struct {
	Params Params `json:"params"`
	Hosts  []Host `json:"hosts"`
}

type Params struct {
	TimeStart     *time.Time `json:"timeStart"`
	TimeEnd       *time.Time `json:"timeEnd"`
	OldestFlapID  *int       `json:"oldestFlapID"`
	FirstFlapTime *time.Time `json:"firstFlapTime"`
	LastFlapTime  *time.Time `json:"lastFlapTime"`
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

func (p *Port) CreateFromDB(e PortDBEntry) {
	p.IfIndex = e.IfIndex
	p.IfName = e.IfName
	p.IfAlias = e.IfAlias
	p.IfOperStatus = e.IfOperStatus
	p.FirstFlapTime = &e.Time
	p.LastFlapTime = &e.Time
	p.FlapCount = 1

}

func (p *Port) updateFromDB(e PortDBEntry) {
	if p.IfIndex != e.IfIndex {
		panic("Wrong usage of Port.UpdateFromDB")
	}
	p.FlapCount++
	p.IfAlias = e.IfAlias
	if e.Time.Before(*p.FirstFlapTime) {
		p.FirstFlapTime = &e.Time
	} else if e.Time.After(*p.LastFlapTime) {
		p.LastFlapTime = &e.Time
	}
}

type Host struct {
	Name      string `json:"name"`
	Ipaddress string `json:"ipaddress"`
	Ports     []Port `json:"ports"`
}

func (h *Host) CreateFromDB(e PortDBEntry) {
	h.Ipaddress = e.Ipaddress
	h.Name = e.Hostname
	port := Port{}
	port.CreateFromDB(e)
	h.Ports = append(h.Ports, port)
}

func (h *Host) UpdateFromDB(e PortDBEntry) {
	if h.Ipaddress != e.Ipaddress {
		panic("Wrong usage of Host.UpdateFromDB")
	}
	h.Name = e.Hostname

	// Decide if we need to update existing port of create new one
	for i, port := range h.Ports {
		if port.IfIndex == e.IfIndex {
			h.Ports[i].updateFromDB(e)
			return
		}
	}
	port := Port{}
	port.CreateFromDB(e)
	h.Ports = append(h.Ports, port)

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

func (f *Flapper) Review(startTime, endTime time.Time) (ReviewResult, error) {

	/*
		{
		  "params": {
		    "timeStart": "2016-01-01 23:00:00",
		    "timeEnd": "2016-01-02 00:00:00",
		    "oldestFlapID": "77472",
		    "firstFlapTime": "2016-01-01 23:10:00",
		    "lastFlapTime": "2016-01-01 23:59:45"
		  },
		  "hosts": [
		    {
		      "name": "Chicago-router1",
		      "ipaddress": "192.168.100.70",
		      "ports": [
		        {
		          "ifIndex": "800",
		          "ifName": "xe-0\/1\/0",
		          "ifAlias": "ChicagoMusicExchange",
		          "ifOperStatus": "up",
		          "flapCount": "1",
		          "firstFlapTime": "2016-01-01 23:10:00",
		          "lastFlapTime": "2016-01-01 23:10:00",
		          "isBlacklisted": false,
		          "flaps": []
		        }
		     ]
		   }
		  ]
		}
	*/

	query := fmt.Sprintf(`SELECT id,
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

	rows, _ := f.db.Query(query)
	for rows.Next() {
		entry := PortDBEntry{}
		if err := rows.Scan(
			&entry.Id,
			&entry.Sid,
			&entry.Time,
			&entry.TimeTicks,
			&entry.Ipaddress,
			&entry.Hostname,
			&entry.IfIndex,
			&entry.IfName,
			&entry.IfAlias,
			&entry.IfAdminStatus,
			&entry.IfOperStatus,
		); err != nil {
			log.Fatal(err)
		}
		if result.Params.OldestFlapID == nil {
			result.Params.OldestFlapID = &entry.Id
		}
		if result.Params.FirstFlapTime == nil {
			result.Params.FirstFlapTime = &entry.Time
		}
		result.Params.LastFlapTime = &entry.Time

		if host.Ipaddress == "" {
			host.CreateFromDB(entry)

		} else if host.Ipaddress == entry.Ipaddress {
			host.UpdateFromDB(entry)

		} else {
			result.Hosts = append(result.Hosts, *host)
			host = &Host{}
			host.CreateFromDB(entry)

		}

	}

	//for _, h := range hosts {
	//	result.Hosts = append(result.Hosts, *h)
	//}
	return result, nil

}

// SERVER

type Server struct {
	flapper *Flapper
}

func (s Server) http404(response http.ResponseWriter, request *http.Request) {
	response.WriteHeader(http.StatusNotFound)
	response.Write([]byte("Resource not found"))
}

func (s *Server) HandleReview(response http.ResponseWriter, request *http.Request) {

	endTime := time.Now()
	startTime := endTime.Add(-3600 * time.Second)
	results, err := s.flapper.Review(startTime, endTime)

	jsonResults, err := json.Marshal(results)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		return
	}
	response.Write(jsonResults)

}

func (s *Server) route(response http.ResponseWriter, request *http.Request) {
	// Fuzzy routing logic caused by bad API design :(
	if strings.HasPrefix(request.URL.RawQuery, "review") {
		s.HandleReview(response, request)
		return
	} else {
		s.http404(response, request)
		return
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
