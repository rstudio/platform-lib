package integrationtest

// Copyright (C) 2025 by Posit Software, PBC

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fortytw2/leaktest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v2/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/internal"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/internal/servertest"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/servers/file"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/servers/postgres"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/servers/s3server"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/types"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

// This suite will be skipped when running tests with SQLite only. To test, use
// the `test-integration` target. To run these tests only, use:
// `MODULE=pkg/rsstorage/internal/integration_test just test-integration -v github.com/rstudio/platform-lib/pkg/rsstorage/internal/integration_test -check.f=StorageIntegrationSuite`
type StorageIntegrationSuite struct {
	pool          *pgxpool.Pool
	tempdirhelper servertest.TempDirHelper
}

var _ = check.Suite(&StorageIntegrationSuite{})

type dummyStore struct {
	pool *pgxpool.Pool
}

func (d *dummyStore) CacheObjectEnsureExists(cacheName, key string) error {
	return nil
}

func (d *dummyStore) CacheObjectMarkUse(cacheName, key string, accessTime time.Time) error {
	return nil
}

func create(dbname string) (err error) {
	if dbname != "postgres" {
		connectionString := fmt.Sprintf("postgres://admin:password@postgres/%s?sslmode=disable", "postgres")

		var conn *pgx.Conn
		ctx := context.Background()
		conn, err = pgx.Connect(ctx, connectionString)
		if err != nil {
			return
		}
		defer conn.Close(ctx)

		_, err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbname))
	}
	return
}

func EphemeralPostgresPool(dbname string) (pool *pgxpool.Pool, err error) {
	err = create(dbname)
	if err != nil {
		return
	}

	connectionString := fmt.Sprintf("postgres://admin:password@postgres/%s?sslmode=disable", dbname)
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return
	}

	config.MaxConns = int32(10)

	pool, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return
	}

	sql := "" +
		"CREATE TABLE large_objects ( " +
		"	oid INTEGER PRIMARY KEY, " +
		"	address TEXT UNIQUE NOT NULL " +
		");"
	_, err = pool.Exec(context.Background(), sql)

	return
}

func (s *StorageIntegrationSuite) SetUpSuite(c *check.C) {
	if testing.Short() {
		c.Skip("skipping integration tests because -short was provided")
	}
}

func (s *StorageIntegrationSuite) SetUpTest(c *check.C) {
	var err error
	dbname := strings.ToLower(internal.RandomString(16)) // databases must be lower case
	s.pool, err = EphemeralPostgresPool(dbname)
	c.Assert(err, check.IsNil)

	c.Assert(s.tempdirhelper.SetUp(), check.IsNil)
}

func (s *StorageIntegrationSuite) TearDownTest(c *check.C) {
	c.Assert(s.tempdirhelper.TearDown(), check.IsNil)
}

// Creates a set of servers that cover all our supported storage subsystems.
// In the tests, we'll typically create two server sets, one set as a source
// set and another set as a destination set.
func (s *StorageIntegrationSuite) NewServerSet(c *check.C, class, prefix string) map[string]rsstorage.StorageServer {

	s3Svc, err := s3server.NewS3Wrapper(
		s3.Options{
			Region:          "us-east-1",
			BaseEndpoint:    aws.String("http://minio:9000"),
			EndpointOptions: s3.EndpointResolverOptions{DisableHTTPS: true},
			UsePathStyle:    true,
			Credentials:     credentials.NewStaticCredentialsProvider("minio", "miniokey", ""),
		},
	)
	c.Assert(err, check.IsNil)

	// Create S3 bucket
	_, err = s3Svc.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(class)})
	c.Assert(err, check.IsNil)

	c.Assert(s.pool, check.NotNil)

	// Prep store
	cstore := &dummyStore{
		pool: s.pool,
	}

	// Prep directory for file storage
	dir, err := ioutil.TempDir(s.tempdirhelper.Dir(), "")
	c.Assert(err, check.IsNil)

	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}

	pgServer := postgres.NewStorageServer(postgres.StorageServerArgs{
		ChunkSize: 100 * 1024,
		Waiter:    wn,
		Notifier:  wn,
		Class:     class,
		Pool:      s.pool,
	})
	s3Server := s3server.NewStorageServer(s3server.StorageServerArgs{
		Bucket:    class,
		Svc:       s3Svc,
		ChunkSize: 100 * 1024,
		Waiter:    wn,
		Notifier:  wn,
	})
	fileServer := file.NewStorageServer(file.StorageServerArgs{
		Dir:          dir,
		ChunkSize:    100 * 1024,
		Waiter:       wn,
		Notifier:     wn,
		Class:        class,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})

	return map[string]rsstorage.StorageServer{
		"file": rsstorage.NewMetadataStorageServer(rsstorage.MetadataStorageServerArgs{
			Name:   "file",
			Server: fileServer,
			Store:  cstore,
		}),
		"s3": rsstorage.NewMetadataStorageServer(rsstorage.MetadataStorageServerArgs{
			Name:   "s3",
			Server: s3Server,
			Store:  cstore,
		}),
		"postgres": rsstorage.NewMetadataStorageServer(rsstorage.MetadataStorageServerArgs{
			Name:   "pg",
			Server: pgServer,
			Store:  cstore,
		}),
	}
}

// The string that will be used to populate each stored asset.
const testAssetData = "this is a test for the server of class %s"

func (s *StorageIntegrationSuite) PopulateServerSet(c *check.C, set map[string]rsstorage.StorageServer) {
	ctx := context.Background()
	resolver := func(class string) types.Resolver {
		return func(w io.Writer) (string, string, error) {
			_, err := w.Write([]byte(fmt.Sprintf(testAssetData, class)))
			return "", "", err
		}
	}

	// Create some chunked data
	szPut := uint64(len(servertest.TestDESC))
	resolverChunked := func(class string) types.Resolver {
		return func(w io.Writer) (string, string, error) {
			buf := bytes.NewBufferString(servertest.TestDESC)
			_, err := io.Copy(w, buf)
			return "", "", err
		}
	}

	// Populate each server in the set with a set of assets. The set of assets on each server
	// is equal in length to the number of servers in the set so we can test transferring
	// assets to all the other server types.
	for class, server := range set {
		for assetClass := range set {
			_, _, err := server.Put(ctx, resolver(class), "", fmt.Sprintf("%s->%s", class, assetClass))
			c.Assert(err, check.IsNil)
			_, _, err = server.Put(ctx, resolver(class), "dir", fmt.Sprintf("%s->%s", class, assetClass))
			c.Assert(err, check.IsNil)
			_, _, err = server.PutChunked(ctx, resolverChunked(class), "chunked", fmt.Sprintf("%s->%s", class, assetClass), szPut)
			c.Check(err, check.IsNil)
		}
	}
}

// Verifies that a given asset exists
func (s *StorageIntegrationSuite) CheckFile(
	c *check.C,
	server rsstorage.StorageServer,
	test string,
	dir string,
	address string,
	classSource string,
	class string,
	sz int64,
	chunked bool,
) {
	log.Printf("(%s) Verifying existence of %s on server=%s (with dir=%s)", test, address, class, dir)

	// Next, get it
	r, ch, sz, _, ok, err := server.Get(context.Background(), dir, address)
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(sz, check.Equals, sz)
	if chunked {
		c.Check(ch, check.NotNil)
	} else {
		c.Check(ch, check.IsNil)
	}

	// Check contents
	bs := &bytes.Buffer{}
	_, err = io.Copy(bs, r)
	c.Assert(err, check.IsNil)
	if chunked {
		c.Check(bs.String(), check.Equals, servertest.TestDESC)
	} else {
		c.Check(bs.String(), check.Equals, fmt.Sprintf(testAssetData, classSource))
	}

	// Close it
	c.Assert(r.Close(), check.IsNil)
}

// Verifies that a given asset does not exist
func (s *StorageIntegrationSuite) CheckFileGone(c *check.C, server rsstorage.StorageServer, test, dir, address, classSource string) {
	log.Printf("(%s) Verifying removal of %s on server=%s (with dir=%s)", test, address, classSource, dir)

	ok, _, _, _, err := server.Check(context.Background(), "", address)
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, false)
}

func (s *StorageIntegrationSuite) TestMoving(c *check.C) {
	ctx := context.Background()
	sources := s.NewServerSet(c, "source-move", "")
	dests := s.NewServerSet(c, "dest-move", "destination")

	s.PopulateServerSet(c, sources)

	// Move files
	for classSource, source := range sources {
		for classDest, dest := range dests {
			err := source.Move(ctx, "", fmt.Sprintf("%s->%s", classSource, classDest), dest)
			c.Assert(err, check.IsNil)
			err = source.Move(ctx, "dir", fmt.Sprintf("%s->%s", classSource, classDest), dest)
			c.Assert(err, check.IsNil)
			err = source.Move(ctx, "chunked", fmt.Sprintf("%s->%s", classSource, classDest), dest)
			c.Assert(err, check.IsNil)
		}
	}

	// Verify
	for classSource := range sources {
		log.Printf("\nVerify that files were successfully moved from %s to each destination server:", classSource)
		for classDest, dest := range dests {
			// Files exist on destination
			s.CheckFile(
				c,
				dest,
				"Move-Dst",
				"",
				fmt.Sprintf("%s->%s", classSource, classDest),
				classSource,
				classDest,
				int64(len(testAssetData)+len(classSource)-2),
				false,
			)
			s.CheckFile(
				c,
				dest,
				"Move-Dst",
				"dir",
				fmt.Sprintf("%s->%s", classSource, classDest),
				classSource,
				classDest,
				int64(len(testAssetData)+len(classSource)-2),
				false,
			)
			s.CheckFile(
				c,
				dest,
				"Move-Dst",
				"chunked",
				fmt.Sprintf("%s->%s", classSource, classDest),
				classSource,
				classDest,
				1953,
				true,
			)
		}
	}

	// Verify that files do not exist on source
	for classSource, source := range sources {
		log.Printf("\nVerify that moved files were deleted from the %s server:", classSource)
		for classDest := range dests {
			s.CheckFileGone(c, source, "Move-Src", "", fmt.Sprintf("%s->%s", classSource, classDest), classSource)
			s.CheckFileGone(c, source, "Move-Src", "dir", fmt.Sprintf("%s->%s", classSource, classDest), classSource)
			s.CheckFileGone(c, source, "Move-Src", "chunked", fmt.Sprintf("%s->%s", classSource, classDest), classSource)
		}
	}
}

func (s *StorageIntegrationSuite) TestCopying(c *check.C) {
	ctx := context.Background()
	sources := s.NewServerSet(c, "source-copy", "")
	dests := s.NewServerSet(c, "dest-copy", "destination")

	s.PopulateServerSet(c, sources)

	// Copy files
	for classSource, source := range sources {
		for classDest, dest := range dests {
			err := source.Copy(ctx, "", fmt.Sprintf("%s->%s", classSource, classDest), dest)
			c.Assert(err, check.NotNil)
			err = source.Copy(ctx, "dir", fmt.Sprintf("%s->%s", classSource, classDest), dest)
			c.Assert(err, check.IsNil)
			err = source.Copy(ctx, "chunked", fmt.Sprintf("%s->%s", classSource, classDest), dest)
			c.Assert(err, check.IsNil)
		}
	}

	// Verify files have been copied to destination
	for classSource := range sources {
		log.Printf("\nVerify that files were successfully copied from %s to each destination server:", classSource)
		for classDest, dest := range dests {
			// Files exist on destination
			s.CheckFile(
				c,
				dest,
				"Copy-Dst",
				"",
				fmt.Sprintf("%s->%s", classSource, classDest),
				classSource,
				classDest,
				int64(len(testAssetData)+len(classSource)-2),
				false,
			)
			s.CheckFile(
				c,
				dest,
				"Copy-Dst",
				"dir",
				fmt.Sprintf("%s->%s", classSource, classDest),
				classSource,
				classDest,
				int64(len(testAssetData)+len(classSource)-2),
				false,
			)
			s.CheckFile(
				c,
				dest,
				"Copy-Dst",
				"chunked",
				fmt.Sprintf("%s->%s", classSource, classDest),
				classSource,
				classDest,
				1953,
				true,
			)
		}
	}

	// Verify files are still on source
	for classSource, source := range sources {
		log.Printf("\nVerify that original files still remain on the %s server:", classSource)
		for classDest := range dests {
			// Files still exist on source
			s.CheckFile(
				c,
				source,
				"Copy-Src",
				"",
				fmt.Sprintf("%s->%s", classSource, classDest),
				classSource,
				classSource,
				int64(len(testAssetData)+len(classSource)-2),
				false,
			)
			s.CheckFile(
				c,
				source,
				"Copy-Src",
				"dir",
				fmt.Sprintf("%s->%s", classSource, classDest),
				classSource,
				classSource,
				int64(len(testAssetData)+len(classSource)-2),
				false,
			)
			s.CheckFile(
				c,
				source,
				"Copy-Src",
				"chunked",
				fmt.Sprintf("%s->%s", classSource, classDest),
				classSource,
				classSource,
				1953,
				true,
			)
		}
	}

	// Test Enumeration
	for classSource, source := range sources {
		log.Printf("\nVerify enumeration on the %s source server:", classSource)
		items, err := source.Enumerate(ctx)
		c.Assert(err, check.IsNil)
		// Each source should have three files for each destination
		c.Assert(items, check.HasLen, len(dests)*3)
	}
	for classDest, dest := range dests {
		log.Printf("\nVerify enumeration on the %s destination server:", classDest)
		items, err := dest.Enumerate(ctx)
		c.Assert(err, check.IsNil)
		// Each destination should have three files from each source
		c.Assert(items, check.HasLen, len(sources)*3)
	}

	// Test Removal
	for classSource, source := range sources {
		log.Printf("\nVerify forced removal of assets on the %s source server:", classSource)
		for classDest := range dests {
			err := source.Remove(ctx, "", fmt.Sprintf("%s->%s", classSource, classDest))
			c.Assert(err, check.IsNil)
			err = source.Remove(ctx, "dir", fmt.Sprintf("%s->%s", classSource, classDest))
			c.Assert(err, check.IsNil)
			err = source.Remove(ctx, "chunked", fmt.Sprintf("%s->%s", classSource, classDest))
			c.Assert(err, check.IsNil)
		}
		items, err := source.Enumerate(ctx)
		c.Assert(err, check.IsNil)
		// Each source now have zero assets
		c.Assert(items, check.HasLen, 0)
	}
}

// Ensures that files are cleaned up, and we don't end up with zero-length files after
// resolver failures.
//
// Run with (against local MinIO instance):
//
//	`MODULE=pkg/rsstorage/internal/integration_test just test-integration -v github.com/rstudio/platform-lib/pkg/rsstorage/internal/integration_test -check.f=S3IntegrationSuite`
//
// To run against your own AWS S3 bucket:
//
//	First, customize the variables in the test below as noted. Then, run:
//	  `MODULE=pkg/rsstorage/internal/integration_test DEF_TEST_ARGS="-v" just test github.com/rstudio/platform-lib/pkg/rsstorage/internal/integration_test -check.f=S3IntegrationSuite`
type S3IntegrationSuite struct{}

var _ = check.Suite(&S3IntegrationSuite{})

func (s *S3IntegrationSuite) SetUpSuite(c *check.C) {
	if testing.Short() {
		c.Skip("skipping s3 integration tests because -short was provided")
	}
}

var minioEndpoint = "http://minio:9000"
var awsEndpoint = ""

func (s *S3IntegrationSuite) TestPopulateServerSetHang(c *check.C) {
	ctx, cancelFn := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelFn()
	defer leaktest.Check(c)

	// Customize these as needed for your environment
	//endpoint := awsEndpoint // AWS
	endpoint := minioEndpoint // MinIO
	bucket := "rsstorage-minio-test"
	region := "us-east-1"

	// Try to use correct settings
	disableSSL := false
	forcePathStyle := false
	if endpoint == minioEndpoint {
		disableSSL = true
		forcePathStyle = true
	}

	s3Svc, err := s3server.NewS3Wrapper(
		s3.Options{
			Region:          region,
			BaseEndpoint:    &endpoint,
			EndpointOptions: s3.EndpointResolverOptions{DisableHTTPS: disableSSL},
			UsePathStyle:    forcePathStyle,
			Credentials:     credentials.NewStaticCredentialsProvider("minio", "miniokey", ""),
		},
	)
	c.Assert(err, check.IsNil)

	// Create S3 bucket if using local MinIO Server
	if endpoint == minioEndpoint {
		_, err = s3Svc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		// Ignore errors if the bucket already exists.
		if err != nil {
			var bae *s3Types.BucketAlreadyExists
			var bao *s3Types.BucketAlreadyOwnedByYou
			if errors.As(err, &bae) || errors.As(err, &bao) {
				err = nil
			}
		}
		c.Assert(err, check.IsNil)
		defer func() {
			s3Svc.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
		}()
	}

	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	s3Server := s3server.NewStorageServer(s3server.StorageServerArgs{
		Bucket:    bucket,
		Prefix:    "integration-test",
		Svc:       s3Svc,
		ChunkSize: 100 * 1024,
		Waiter:    wn,
		Notifier:  wn,
	})

	// This channel gets notified/closed after we start writing some data to the writer,
	// but before the write fails.
	writing := make(chan struct{})

	// This channel gets notified/closed when we're ready for the resolve function to fail.
	end := make(chan struct{})

	resolver := func(class string) types.Resolver {
		return func(w io.Writer) (string, string, error) {
			// Start writing some data to the resolver's writer
			//w.Write([]byte(fmt.Sprintf(testAssetData, class)))
			gzw := gzip.NewWriter(w)

			log.Printf("resolver: wrote some data, instructing test to continue, but waiting for instruction to err")
			// Notify that we've started to write some data
			close(writing)

			// Wait until the test is ready for us to fail, then fail
			<-end
			log.Printf("resolver: returning error")
			// Emulates the behavior of returning an error before the deferred
			// call to close the gzip writer
			if true {
				return "", "", errors.New("failure resolving data")
			}

			defer gzw.Close()
			return "", "", nil
		}
	}

	// This channel gets notified/closed after the resolve fails and the `Put` fails.
	failed := make(chan struct{})
	itemAddress := uuid.New().String()
	go func() {
		// Notify that we're done with the Put call
		defer close(failed)
		log.Printf("put: adding an item to S3 in a separate goroutine")
		// Put an item into S3. This will fail
		_, _, err = s3Server.Put(ctx, resolver("test-failure"), "", itemAddress)
		c.Assert(err, check.NotNil)
		c.Assert(strings.HasSuffix(err.Error(), "failure resolving data"), check.Equals, true)
	}()

	// Don't attempt anything until we've started attempting to write to S3
	log.Printf("get: waiting for write to start")
	<-writing

	// Check to see if we can find the item that we're writing
	log.Printf("get: attempting to get item that is being written")
	_, _, _, _, ok, err := s3Server.Get(ctx, "", itemAddress)
	c.Check(err, check.NotNil)
	c.Check(ok, check.Equals, false)

	// Notify the writer/resolver to fail now
	close(end)

	// Wait for failure, and test again to ensure item is still gone
	<-failed

	// Check again to see if we can find the failed item.
	_, _, _, _, ok, err = s3Server.Get(ctx, "", itemAddress)
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, false)
}

func (s *S3IntegrationSuite) TestPopulateServerSetHangChunked(c *check.C) {
	ctx, cancelFn := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelFn()
	defer leaktest.Check(c)

	// Customize these as needed for your environment
	//endpoint := awsEndpoint // AWS
	endpoint := minioEndpoint // MinIO
	bucket := "rsstorage-minio-test"
	region := "us-east-1"

	// Try to use correct settings
	disableSSL := false
	forcePathStyle := false
	if endpoint == minioEndpoint {
		disableSSL = true
		forcePathStyle = true
	}

	// commenting out the credentials for now so the test fails quickly
	// instead of hanging up
	s3Svc, err := s3server.NewS3Wrapper(
		s3.Options{
			Region:          region,
			BaseEndpoint:    &endpoint,
			EndpointOptions: s3.EndpointResolverOptions{DisableHTTPS: disableSSL},
			UsePathStyle:    forcePathStyle,
			Credentials:     credentials.NewStaticCredentialsProvider("minio", "miniokey", ""),
		},
	)
	c.Assert(err, check.IsNil)

	// Create S3 bucket if using local MinIO Server
	if endpoint == minioEndpoint {
		_, err = s3Svc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		// Ignore errors if the bucket already exists.
		if err != nil {
			var bae *s3Types.BucketAlreadyExists
			var bao *s3Types.BucketAlreadyOwnedByYou

			if errors.As(err, &bae) || errors.As(err, &bao) {
				err = nil
			}
		}
		c.Assert(err, check.IsNil)
		defer func() {
			s3Svc.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
		}()
	}

	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	s3Server := s3server.NewStorageServer(s3server.StorageServerArgs{
		Bucket:    bucket,
		Prefix:    "integration-test",
		Svc:       s3Svc,
		ChunkSize: 100 * 1024,
		Waiter:    wn,
		Notifier:  wn,
	})

	// This channel gets notified/closed after we start writing some data to the writer,
	// but before the write fails.
	writing := make(chan struct{})

	// This channel gets notified/closed when we're ready for the resolve function to fail.
	end := make(chan struct{})

	resolver := func(class string) types.Resolver {
		return func(w io.Writer) (string, string, error) {
			// Start writing some data to the resolver's writer
			w.Write([]byte(fmt.Sprintf(testAssetData, class)))
			gzw := gzip.NewWriter(w)

			log.Printf("resolver: wrote some data, instructing test to continue, but waiting for instruction to err")
			// Notify that we've started to write some data
			close(writing)

			// Wait until the test is ready for us to fail, then fail
			<-end
			log.Printf("resolver: returning error")
			// Emulates the behavior of returning an error before the deferred
			// call to close the gzip writer
			if true {
				return "", "", errors.New("failure resolving data")
			}

			defer gzw.Close()
			return "", "", nil
		}
	}

	// This channel gets notified/closed after the resolve fails and the `Put` fails.
	failed := make(chan struct{})
	itemAddress := uuid.New().String()
	go func() {
		// Notify that we're done with the Put call
		defer close(failed)
		log.Printf("put: adding a chunked item to S3 in a separate goroutine")
		// Put an item into S3. This will fail
		_, _, err = s3Server.PutChunked(ctx, resolver("test-failure"), "", itemAddress, 100*1024)
		c.Assert(err, check.ErrorMatches, "failure resolving data")
	}()

	// Don't attempt anything until we've started attempting to write to S3
	log.Printf("get: waiting for write to start")
	<-writing

	// Check to see if we can find the item that we're writing. Since this
	// is chunked data, it should appear now, even though it is incomplete.
	log.Printf("get: attempting to get item that is being written")
	_, _, _, _, ok, err := s3Server.Get(ctx, "", itemAddress)
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, true)

	// Notify the writer/resolver to fail now
	close(end)

	// Wait for failure, and test again to ensure item is still gone
	<-failed

	// Check again to see if we can find the failed item. It should have been
	// cleaned up when the Put failed.
	_, _, _, _, ok, err = s3Server.Get(ctx, "", itemAddress)
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, false)
}
