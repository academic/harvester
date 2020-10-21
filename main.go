package main

import (
	"fmt"

	"github.com/academic/harvester/oai"
)

func main() {
	fmt.Println("Hello Academic Harvester!")
	req := &oai.Request{
		BaseURL: "http://www.archive.org/services/oai2.php"}

	req.HarvestRecords(func(record *oai.Record) {
		fmt.Printf("%s\n\n", record.Metadata.Body[0:100])
	})

}
