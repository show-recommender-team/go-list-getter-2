package etl

import (
	"bytes"
	"container/list"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"time"

	"astuart.co/goq"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cenkalti/backoff"
	"github.com/nleeper/goment"
	"github.com/nokusukun/jikan2go/user"
)

type MALUsers struct {
	Users []string `goquery:"div#content table tbody tr td table tbody tr td div:first-child > a,text"`
}

type AnimeRating struct {
	ID      int64   `json: "id"`
	Title   string  `json: "title"`
	Score   int64   `json: "user_score"`
	Watched float64 `json: "watched_per"`
}

type UserList struct {
	UID      int64         `json:"uid"`
	Username string        `json: "username"`
	List     []AnimeRating `json: "list"`
}

type RunnerProvider struct {
	Bucket  *string
	Session *session.Session
	Quit    chan struct{}
	Cron    *time.Ticker
}

func NewRunner(bucket *string, session *session.Session, cron *time.Ticker) *RunnerProvider {
	r := new(RunnerProvider)
	r.Quit = make(chan struct{})
	r.Bucket = bucket
	r.Session = session
	r.Cron = cron
	return r
}

func NewUserListRequestBuilder(aL *user.AnimeList, u *user.User) func() error {
	return func() error {
		aLD, err := user.GetAnimeList(*u, user.AllList)
		if err != nil {
			fmt.Printf("------Request Error List Builder------\n%+v\n", err)
			if reflect.TypeOf(aLD).String() != "AnimeList" {
				fmt.Printf("------Raw Response------\n%+v\n", aLD)
			}
			if err.Error() != "too many requests" {
				err2 := new(backoff.PermanentError)
				err2.Err = err
				return err2
			}
			return err
		}
		*aL = aLD
		return nil
	}
}

func NewUserListRequestWithBackoff(aL *user.AnimeList, u *user.User) error {
	back := backoff.NewExponentialBackOff()
	return backoff.Retry(NewUserListRequestBuilder(aL, u), back)
}

func NewUserRequestBuilder(u *user.User, uname string) func() error {
	return func() error {
		uD, err := user.GetUser(user.User{Username: uname})
		if err != nil {
			fmt.Printf("------Request Error User Builder------\nUsername: %v\n-------------------------\n%+v\n", uname, err)
			if err.Error() == "bad request" {
				err2 := new(backoff.PermanentError)
				err2.Err = err
				return err2
			}
			return err
		}
		*u = uD
		return nil
	}
}

func NewUserRequestWithBackoff(u *user.User, uname string) error {
	back := backoff.NewExponentialBackOff()
	return backoff.Retry(NewUserRequestBuilder(u, uname), back)
}

func (r *RunnerProvider) doWork() {
	data := r.GetJSONReviews()
	r.WriteToS3(data)
}

func (r *RunnerProvider) GetJSONReviews() []byte {
	res, err := http.Get("https://myanimelist.net/users.php")
	if err != nil {
		log.Fatalf("%+v", err)
	}
	defer res.Body.Close()
	var us MALUsers
	err = goq.NewDecoder(res.Body).Decode(&us)
	if err != nil {
		fmt.Printf("%+v\n", err)
	}
	userListData := list.New()
	for _, v := range us.Users {
		if v == "" {
			continue
		}
		fmt.Printf("----------\nProcessing lists of %v\n", v)
		u := new(user.User)
		err := NewUserRequestWithBackoff(u, v)
		if err != nil {
			continue
		}
		if u.UserID == 0 {
			fmt.Printf("------User Issue------\nUsername: %v, UID: 0\n", v)
		}
		uData := UserList{UID: u.UserID, Username: u.Username}
		uHist := new(user.AnimeList)
		err = NewUserListRequestWithBackoff(uHist, u)
		if err != nil {
			continue
		}
		aList := uHist.Anime
		aListData := make([]AnimeRating, len(aList))
		for i2, v2 := range aList {
			var compositeScore int64
			if v2.Score <= 0 {
				compositeScore = -1
			} else {
				compositeScore = v2.Score
			}
			var per float64
			if v2.TotalEpisodes <= 0 {
				fmt.Printf("------Weird Total Eps------\nAnime: %v\nTotal: %v\nWatched: %v\n",
					v2.Title, v2.TotalEpisodes, v2.WatchedEpisodes)
				per = -1
			} else {
				per = float64(v2.WatchedEpisodes) / float64(v2.TotalEpisodes)
			}
			aRev := AnimeRating{
				ID:      v2.MalID,
				Title:   v2.Title,
				Score:   compositeScore,
				Watched: per,
			}
			aListData[i2] = aRev
		}
		uData.List = aListData
		userListData.PushBack(uData)
	}
	uListSlice := make([]UserList, userListData.Len())
	i := 0
	for current := userListData.Front(); current.Next() != nil; current = current.Next() {
		uListSlice[i] = current.Value.(UserList)
		i++
	}
	j, err := json.Marshal(uListSlice)
	if err != nil {
		log.Fatal(err)
	}
	return j
}

func (r *RunnerProvider) WriteToS3(content []byte) error {
	uploader := s3manager.NewUploader(r.Session)
	now, _ := goment.New()
	fname := fmt.Sprintf("%v-out.json", now.Format("DDMMYYYY-HHmmss"))
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: r.Bucket,
		Key:    aws.String(fname),
		Body:   bytes.NewReader(content),
		ACL:    aws.String("public-read"),
	})
	return err
}

func (r *RunnerProvider) Do() {
	do := func() {
		for {
			select {
			case <-r.Cron.C:
				r.doWork()
			case <-r.Quit:
				r.Cron.Stop()
				return
			}
		}
	}
	go do()
}
