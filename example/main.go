package main

import (
	"fmt"
	"gotest/msgspub"
)

func main() {
	var spubor messpub.Spubor
	err := spubor.Init()
	if err != nil {
		fmt.Println("spubor init err:", err)
	}
	var subjects = 1024
	var subitems = 1024
	var pubitems = 256

	for i := 0; i < subjects; i++ {
		var msgid = fmt.Sprintf("testmsgid%d", i)
		for j := 0; j < subitems; j++ {
			idx, ch, err := spubor.SubMes(msgid)
			if err != nil {
				fmt.Println("submes err:", err)
				continue
			}
			go func(index int64, ch chan string) {
				for {
					select {
					case msg := <-ch:
						fmt.Printf("recvmsg: index:%d,msg:%s\r\n", index, msg)
					}
				}
			}(idx, ch)
		}
	}

	go func() {
		for i := 0; i < subjects; i++ {
			var msgid = fmt.Sprintf("testmsgid%d", i)
			for j := 0; j < pubitems; j++ {
				err := spubor.PubMes(msgid, fmt.Sprintf("%s,halo%d", msgid, i))
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}()
	select {}
}
