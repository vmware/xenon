# About

Demonstrate subscription.

## Commands

List registered subscriptions

```
curl localhost:8000/me/subscriptions
```

Create a new document

```
curl localhost:8000/me -X POST -H "content-type: application/json" -d "{}"
```