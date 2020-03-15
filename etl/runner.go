package etl

import (
	"fmt"
	"net/http"

	"astuart.co/goq"
)

type MALUsers struct {
	Users []string `goquery:"div table tbody tr td table tbody tr td div a,text"`
}

func Do() {
	res, _ := http.Get("https://myanimelist.net/users.php")
	defer res.Body.Close()
	var us MALUsers
	err := goq.NewDecoder(res.Body).Decode(&us)
	if err != nil {
		fmt.Printf("%+v\n", err)
	}
	fmt.Printf("%+v\n", us)
}
