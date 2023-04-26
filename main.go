package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	// "github.com/k0kubun/pp/v3"
	"gopkg.in/yaml.v3"
)

type App struct {
	AppName    string `yaml:"app_name"`
	Tables     []string `yaml:"tables"`
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	Database   string `yaml:"database"`
	Host       string `yaml:"host"`
	OutputPath string `yaml:"output_path"`
}

func main() {
	cfg := flag.String("cfg", "", "path to apps file")
	logFile := flag.String("log-file", "./error.log", "path to error log file")
	flag.Parse()

	fLog, err := os.OpenFile(*logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer fLog.Close()

	log.SetOutput(fLog)

	fmt.Printf("log path is %q\n", *logFile)

	if *cfg == "" {
		log.Fatalf("-cfg flag is not set")
		return
	}

	fp, err := os.ReadFile(*cfg)
	if err != nil {
		log.Fatalf("error in reading apps file: %v\n", err)
	}

	var apps []App
	err = yaml.Unmarshal(fp, &apps)
	if err != nil {
		log.Fatalf("error in unmarshal yaml file to apps: %v\n", err)
	}

	// pp.Println(apps)

	for _, app := range apps {
		postFix := time.Now().Format("200601021504")

		args := []string {
			"--column-statistics=0", 
			"-u", app.User,
			"-h", app.Host,
			app.Password, 
			app.Database,
		}
		args = append(args, app.Tables...)
		cmd := exec.Command("mysqldump", args...)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Printf("error in stdout_pipe: %v\n", err)
		}

		if err = cmd.Start(); err != nil {
			log.Printf("error in stdout_pipe: %v\n", err)
		}

		currentMonth := time.Now().Format("2006-01")
		newPath := filepath.Join(app.OutputPath, currentMonth)
		if _, err = os.Stat(newPath); os.IsNotExist(err) {
			err = os.Mkdir(newPath, 0777)
			if err != nil {
				log.Printf("error in creating directory: %v\n", err)
			}
		}
		dumpLocalPath := filepath.Join(app.OutputPath, currentMonth, app.AppName+"_"+postFix+".sql.gz")

		// pp.Println(dumpLocalPath)

		bytes, err := ioutil.ReadAll(stdout)
		if err != nil {
			log.Printf("error in ReadAll: %v\n", err)
		}

		// err = ioutil.WriteFile(dumpLocalPath, bytes, 0644)
		// if err != nil {
		// 	log.Printf("error in saving result in the file: %v\n", err)
		// }

		gzippedFile, err := os.Create(dumpLocalPath)
		if err != nil {
			log.Printf("error in creating %q: %v\n", dumpLocalPath, err)
		}
		defer gzippedFile.Close()

		gzipWriter := gzip.NewWriter(gzippedFile)
		defer gzipWriter.Close()

		_, err = gzipWriter.Write(bytes)
		if err != nil {
			log.Printf("error in gzipWriter: %v\n", err)
		}

		gzipWriter.Flush()

	}
}
