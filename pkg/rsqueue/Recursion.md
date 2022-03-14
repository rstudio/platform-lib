# Recursion and Safe Shutdown in Queue

Recursion becomes a problem when queue work depends on other queue work. Recursion is not an issue if work simply
creates more work, but it is an issue when the work "waits" on additional queue work in order to continue. If the queue
becomes full or if the service is shutting down, the work that is blocked may be blocked forever since the queue is
unable to perform the additional work.


## Recursion Example

Here is an example of a runner that recursively retrieves a package graph.

> This example may be outdated.

```go
func (r *RunnerSample) Run(work queue.RecursableWork) error {

	job := &ParentJob{}
	json.Unmarshal(work.Work, &job)

	_, _, err := r.server.Put(func(writer io.Writer) (string, string, error) {

        // Causes recursion since this work cannot continue until the work that
        // gets the dependency completes.
        dep := r.GetDependency("something")

        // Trimmed for brevity
        _, _ = writer.Write(dep.Bytes())
        return "", "", nil
		
	}, job.Dir(), job.Address())
	return err
}

func (r *RunnerSample) GetDependency(name string) (result *Dependency) {
	spec := cache.ResolverSpec{
		Work: &DependencyJob{
			Name: name,
		},
	}

	// Causes recursion by getting a dependent item from the cache, which
	// in turn uses the queue.
	_, _ := r.objectCache.Get(spec, result)
}
```

## Using Context to Detect Recursion

The queue agent (see `agent.go`) creates a new
context that is attached to the work passed to the work runner. This context includes
a `CtxRecurseData` struct that indicates the work type and provides a `RecurseFunc` 
function that can be used to perform recursive work safely.

### Context Details

Here are the types and the function used to create the context that is attached to
the work:

```go
type RecurseFunc func(run func())

type CtxRecurseData struct {
	Recurse  RecurseFunc
	WorkType uint64
}

func ContextWithRecursion(ctx context.Context, workType uint64, recurseFunc RecurseFunc) context.Context {
	return context.WithValue(ctx, CtxRecurse, &CtxRecurseData{Recurse: recurseFunc, WorkType: workType})
}
```

### How Queue Agent Creates Context for Work

The queue agent runs the work like this, attaching the context:

> See `queue/agent/agent.go` for the default recurse function indicated by 
> `a.recurse`, below.

```go
err := a.runner.Run(queue.RecursableWork{
	Work:     queueWork.Work,
	WorkType: queueWork.WorkType,
	Context:  queue.ContextWithRecursion(context.Background(), queueWork.WorkType, a.recurse),
})
```

### How Runner Passes Context to Recursive Work

Now, we can make our runner context-aware so that recursive calls to the queue are handled by the `RecurseFunc` that is
part of the context:

<pre>
func (r *RunnerSample) Run(work queue.RecursableWork) error {

	job := &PackagesCurrentWork{}
	json.Unmarshal(work.Work, &job)

	_, _, err := r.server.Put(r.getResolverText(job, <b>work.Context</b>), job.Dir(), job.Address())
	return err
}

func (r *RunnerSample) getResolverText(job *PackagesCurrentWork, <b>ctx context.Context</b>) storage.Resolver {

	return func(writer io.Writer) (string, string, error) {

		// Causes recursion since this work cannot continue until the work that
		// generates the package graph is complete.
		GetGraph(<b>ctx,</b> job.TranId, job.SourceIds...)

		// Trimmed for brevity
		_, _ = writer.Write([]byte("something"))
		return "", "", nil
	}
}

func (r *RunnerSample) GetGraph(<b>ctx context.Context,</b> tx uint64, sourceIds ...uint64) {
	spec := cache.ResolverSpec{
		Work: &graphrunner.GraphWork{
			SourceIds: sourceIds,
			TranId:    tx,
		},
	}
	<b>// Causes recursion by getting a package graph from the cache
	_, _ := r.objectCache.Get(ctx, spec, &source.PackageGraphExport{})</b>
}
</pre>

Note how we pass the context to the `objectCache.Get` function, above. This context is passed down to the `FileCache`,
which checks the context to see if needs to use the recurse function, like this:

<pre>
func (o *fileCache) GetWithSize(ctx context.Context, resolver ResolverSpec) (reader io.ReadCloser, size int64, modTime time.Time, err error) {

	<b>o.recurser.OptionallyRecurse(ctx, func() {</b>

		// Push a job into the queue. AddressedPut is a no-op if the queue
		// already contains an item with the same address.
		err = o.queue.AddressedPush(resolver.Priority, resolver.GroupId, resolver.Address(), resolver.Work)

		// Wait for work to complete
		// ...trimmed for brevity...

	})
	return
}
</pre>

The `OptionallyRecurse` function mentioned above looks at the context to determine whether nor not to use the
`RecurseFunc` function:

```go
func (a *OptionalRecurser) OptionallyRecurse(ctx context.Context, run func()) {
	recurse := ctx.Value(CtxRecurse)
	if recurse != nil {
		if r, ok := recurse.(*CtxRecurseData); ok {
			r.Recurse(run)
			return
		}
	}

	// Catch all: handle work without wrapping
	run()
}
```

## Expecting Recursion

While the recursion-aware context is great for detecting recursion and ensuring that we don't deadlock due to recursion,
it doesn't make the queue aware of work that will _potentially_ recurse before the recursion happens. This awareness is
critical, since we need to know which work will _potentially_ recurse in order to safely stop/flush the queue. For
example, when stopping the queue, we need to wait for all work that will recurse to complete before we can safely stop
accepting any work, since we don't know what additional work will be required due to the recursion. Since the recursion
could happen at any point during the work, we need to know from the start that a given job will _potentially_ recurse
since the recursive call may not have yet occurred.

To support more awareness of recursion, we do two things:

* Add a new value to the context that makes the context aware that recursion can or will occur at some point
  during this work.
* Add logging to the `OptionallyRecurse` method that notifies the log if unexpected recursion occurs.
* Add a `Debug.FatalUnexpectedRecursion` boolean setting that causes a fatal error if unexpected recursion
  occurs. This settings is enabled during dev and during e2e testing to act as a linter to catch
  unexpected recursion early on.

Here's an example of adding the recursion awareness to the context when the work begins:

<pre>
func (r *RunnerSample) getResolverText(job *PackagesCurrentWork, ctx context.Context) storage.Resolver {

	return func(writer io.Writer) (string, string, error) {

		<b>// Flags the context so the recursion will be expected.
		ctx = queue.ContextWithExpectedRecursion(ctx)</b>

		// Causes recursion since this work cannot continue until the work that
		// generates the package graph is complete.
		GetGraph(ctx, job.TranId, job.SourceIds...)

		// Trimmed for brevity
		_, _ = writer.Write([]byte("something"))
		return "", "", nil
	}
}
</pre>

The `OptionallyRecurse` method is now aware of the new `AllowsRecursion` context value, and, when
recursing, it makes sure this value is set. If the value is not set, it either logs a warning or
fails fatally:

<pre>
func (a *OptionalRecurser) OptionallyRecurse(ctx context.Context, run func()) {
	recurse := ctx.Value(CtxRecurse)
	if recurse != nil {
		if r, ok := recurse.(*CtxRecurseData); ok {

			<b>// Are we expecting recursion?
			allowed := ctx.Value(CtxAllowsRecursion)
			if allowed == nil {
				msg := fmt.Sprintf("Work with type %d attempted recursion without being marked for recursion", r.WorkType)
				if a.fatalRecurseCheck {
					log.Fatalf(msg)
				} else {
					log.Printf(msg)
				}
			}</b>

			r.Recurse(run)
			return
		}
	}

	// Catch all: handle work without wrapping
	run()
}
</pre>

## Safe Shutdown

When shutting down, we'll follow roughly these steps:

1. Wait for all work that is flagged with `CtxAllowsRecursion` in the context to complete.
1. Wait for all work running in the context of the `RecurseFunc` function to complete. This may help to flush any work
   that unexpectedly recurses.
1. Stop accepting new queue work.
1. Wait for all remaining in-progress work to complete.
