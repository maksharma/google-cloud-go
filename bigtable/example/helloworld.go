package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/bigtable"
	"go.opencensus.io/exporter/stackdriver"
	ocgrpc "go.opencensus.io/plugin/grpc"
	"go.opencensus.io/stats"
	"go.opencensus.io/trace"
	"go.opencensus.io/zpages"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	tableName        = "Hello-Bigtable"
	columnFamilyName = "cf1"
	columnName       = "greeting"
)

var greetings = []string{"Hello World!", "Hello Cloud Bigtable!", "Hello golang!"}

func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}

func main() {
	project := flag.String("project", "", "The Google Cloud Platform project ID. Required.")
	instance := flag.String("instance", "", "The Google Cloud Bigtable instance ID. Required.")
	flag.Parse()

	for _, f := range []string{"project", "instance"} {
		if flag.Lookup(f).Value.String() == "" {
			log.Fatalf("The %s flag is required.", f)
		}
	}

	ctx := context.Background()
	e, err := stackdriver.NewExporter(stackdriver.Options{ProjectID: *project})
	if err != nil {
		log.Fatalf("Could not create stackdriver exporter %v", err)
	}
	trace.RegisterExporter(trace.Exporter(e))
	stats.RegisterExporter(e)

	// Start server on port 8080 for tracing data
	go func() { log.Fatal(http.ListenAndServe(":8080", nil)) }()
	zpages.AddDefaultHTTPHandlers()

	// always sample in the example
	trace.SetDefaultSampler(trace.AlwaysSample())

	ctx = bigtable.TraceStartSpan(ctx, "cloud.google.com/go/bigtable/example.helloworld")
	doHelloWorld(ctx, *project, *instance)
	bigtable.TraceEndSpan(ctx, err)

	// sleep for 60 sec to see the output at http://localhost:8080/tracez
	fmt.Println("Pausing to view trace at http://localhost:8080/tracez")
	time.Sleep(60 * time.Second)
}

func openCensusOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithStatsHandler(ocgrpc.NewClientStatsHandler())),
	}
}

// Connects to Cloud Bigtable, runs basic operations and print the results.
func doHelloWorld(ctx context.Context, project, instance string) {

	tableCount, err := stats.NewMeasureInt64("cloud.google.com/go/bigtable/example.helloworld/numtables", "count of tables", "Num")
	if err != nil {
		log.Fatalf("Video size measure not created: %v", err)
	}

	// Create view to see the processed table count cumulatively.
	view, err := stats.NewView(
		"cloud.google.com/go/bigtable/example.helloworld/views/num_table_view",
		"count of tables",
		nil,
		tableCount,
		stats.CountAggregation{},
		stats.Cumulative{},
	)
	if err != nil {
		log.Fatalf("Cannot create view: %v", err)
	}

	// Set reporting period to report data at every second.
	stats.SetReportingPeriod(1 * time.Second)

	// Subscribe will allow view data to be exported.
	// Once no longer need, you can unsubscribe from the view.
	if err := view.Subscribe(); err != nil {
		log.Fatalf("Cannot subscribe to the view: %v", err)
	}
	defer func() { view.Unsubscribe() }()

	adminClient, err := bigtable.NewAdminClient(ctx, project, instance)
	if err != nil {
		log.Fatalf("Could not create admin client: %v", err)
	}

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		log.Fatalf("Could not fetch table list: %v", err)
	}

	if !sliceContains(tables, tableName) {
		bigtable.TracePrintf(ctx, nil, "Creating a new table")
		log.Printf("Creating table %s", tableName)
		if err := adminClient.CreateTable(ctx, tableName); err != nil {
			log.Fatalf("Could not create table %s: %v", tableName, err)
		}
	}

	// Record data points.
	stats.Record(ctx, tableCount.M(int64(len(tables)+1)))

	tblInfo, err := adminClient.TableInfo(ctx, tableName)
	if err != nil {
		log.Fatalf("Could not read info for table %s: %v", tableName, err)
	}

	if !sliceContains(tblInfo.Families, columnFamilyName) {
		bigtable.TracePrintf(ctx, nil, "Creating column family")
		if err := adminClient.CreateColumnFamily(ctx, tableName, columnFamilyName); err != nil {
			log.Fatalf("Could not create column family %s: %v", columnFamilyName, err)
		}
	}

	client, err := bigtable.NewClient(ctx, project, instance, openCensusOptions()...)
	if err != nil {
		log.Fatalf("Could not create data operations client: %v", err)
	}

	tbl := client.Open(tableName)
	muts := make([]*bigtable.Mutation, len(greetings))
	rowKeys := make([]string, len(greetings))

	log.Printf("Writing greeting rows to table")
	for i, greeting := range greetings {
		muts[i] = bigtable.NewMutation()
		muts[i].Set(columnFamilyName, columnName, bigtable.Now(), []byte(greeting))

		rowKeys[i] = fmt.Sprintf("%s%d", columnName, i)
	}

	rowErrs, err := tbl.ApplyBulk(ctx, rowKeys, muts)
	if err != nil {
		log.Fatalf("Could not apply bulk row mutation: %v", err)
	}
	if rowErrs != nil {
		for _, rowErr := range rowErrs {
			log.Printf("Error writing row: %v", rowErr)
		}
		log.Fatalf("Could not write some rows")
	}

	log.Printf("Getting a single greeting by row key:")
	row, err := tbl.ReadRow(ctx, rowKeys[0], bigtable.RowFilter(bigtable.ColumnFilter(columnName)))
	if err != nil {
		log.Fatalf("Could not read row with key %s: %v", rowKeys[0], err)
	}
	log.Printf("\t%s = %s\n", rowKeys[0], string(row[columnFamilyName][0].Value))

	log.Printf("Reading all greeting rows:")
	err = tbl.ReadRows(ctx, bigtable.PrefixRange(columnName), func(row bigtable.Row) bool {
		item := row[columnFamilyName][0]
		log.Printf("\t%s = %s\n", item.Row, string(item.Value))
		return true
	}, bigtable.RowFilter(bigtable.ColumnFilter(columnName)))

	if err = client.Close(); err != nil {
		log.Fatalf("Could not close data operations client: %v", err)
	}

	log.Printf("Deleting the table")
	if err = adminClient.DeleteTable(ctx, tableName); err != nil {
		log.Fatalf("Could not delete table %s: %v", tableName, err)
	}

	if err = adminClient.Close(); err != nil {
		log.Fatalf("Could not close admin client: %v", err)
	}
}
