package main

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/dfuse-io/dstore"
)

func main() {

	file, src, dst, err := parseArgs(os.Args)
	if err != nil {
		usage(err)
	}

	r, err := src.OpenObject(file)
	if err != nil {
		fmt.Println("Error reading: ", err)
		os.Exit(1)
	}

	err = dst.WriteObject(file, r)
	if err != nil {
		fmt.Println("Error writing: ", err)
		os.Exit(1)
	}

}

func parseArgs(args []string) (filename string, src dstore.Store, dst dstore.Store, err error) {
	if len(os.Args) < 3 {
		err = fmt.Errorf("not enough arguments")
		return
	}
	var srcURL *url.URL
	srcURL, err = url.Parse(args[1])
	if err != nil {
		return
	}
	filename = filepath.Base(srcURL.Path)
	srcURL.Path = filepath.Dir(srcURL.Path)
	src, err = dstore.NewSimpleStore(srcURL.String())
	if err != nil {
		return
	}
	dst, err = dstore.NewSimpleStore(args[2])
	return
}

func usage(err error) {
	if err != nil {
		fmt.Println("Error: \n", err)
	}
	fmt.Printf("Usage: %s {remote_file} {local_dir}\n", os.Args[0])
	os.Exit(1)
}
