package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/emin/spacedb"
	"github.com/emin/spacedb/helpers"
)

func main() {

	memProf := flag.String("memprofile", "", "write memory profile to this file")
	cpuProf := flag.String("cpuprofile", "", "write cpu profile to this file")
	flag.Parse()

	if *cpuProf != "" {
		f, err := os.Create(*cpuProf)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	dbPath := "test-db/"
	db := spacedb.New(dbPath)

	//now := time.Now()
	//for i := 0; i < 1000000; i++ {
	//	k := fmt.Sprintf("k%v", i)
	//	v := fmt.Sprintf("value = %v", i)
	//	db.Set([]byte(k), []byte(v))
	//}
	//helpers.TimeTrack("insertion took", now)

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		cmd := scanner.Text()
		cont := runCmd(db, cmd)
		if !cont {
			break
		}
		fmt.Print("> ")
	}

	if *memProf != "" {
		f, err := os.Create(*memProf)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
		return
	}
}

func runCmd(db spacedb.SpaceDB, cmd string) bool {
	defer helpers.TimeTrack("query", time.Now())

	cmd = strings.Trim(cmd, " ")
	parts := strings.Split(cmd, " ")

	if parts[0] == "get" {
		res := db.Get([]byte(parts[1]))
		if res == nil {
			fmt.Printf("%v not found\n", parts[1])
		} else {
			if res.IsDeleted {
				fmt.Printf("%v not found\n", parts[1])
			} else {
				fmt.Println(string(res.Value))
			}
		}
	} else if parts[0] == "set" && len(parts) == 3 {
		k := []byte(parts[1])
		v := []byte(parts[2])
		err := db.Set(k, &spacedb.DBValue{
			Value: v,
		})
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("success")
		}
	} else if parts[0] == "delete" {
		res := db.Delete([]byte(parts[1]))
		if res != nil {
			fmt.Printf("%v not found\n", parts[1])
		} else {
			fmt.Println("deleted")
		}
	} else if cmd == "memory" {
		helpers.PrintMemUsage()
	} else if cmd == "load" {
		for i := 0; i < 10_000_000; i++ {
			k := fmt.Sprintf("k%v", i)
			v := fmt.Sprintf("value = %v", i)
			db.Set([]byte(k), &spacedb.DBValue{Value: []byte(v)})
		}
	} else if cmd == "count" {
		fmt.Printf("Estimated Key Count: %v\n", db.KeyCount())
	} else if cmd == "test" {
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("key%v", i)
			v := fmt.Sprintf("value%v", i)
			db.Set([]byte(k), &spacedb.DBValue{Value: []byte(v)})
		}
	} else if cmd == "exit" {
		db.Close()
		fmt.Println("bye..")
		return false
	}

	return true
}
