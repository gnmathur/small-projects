# KV Store

## Test

```
mvn test
```

## Build and run
```
mvn compile && mvn exec:java
```

Key not found: b
> read c
Key not found: c
> write a value-a0
> start
> write b value-b1
> delete c
> commit
> delete c
Key not found: c
> 