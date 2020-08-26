package libdy

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cenkalti/backoff"
)

func query(svc *dynamodb.DynamoDB, table string, input *dynamodb.QueryInput) ([]map[string]*dynamodb.AttributeValue, error) {
	start := time.Now()
	ret := []map[string]*dynamodb.AttributeValue{}
	var lastKey map[string]*dynamodb.AttributeValue
	more := true

	// Could be paginated.
	for more {
		if lastKey != nil {
			input.ExclusiveStartKey = lastKey
		}

		var rerr, err error
		var res *dynamodb.QueryOutput

		// Our retriable, backoff-able function.
		op := func() error {
			res, err = svc.Query(input)
			rerr = err
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case dynamodb.ErrCodeProvisionedThroughputExceededException:
						return err // will cause retry with backoff
					}
				}
			}

			return nil // final err is rerr
		}

		err = backoff.Retry(op, backoff.NewExponentialBackOff())
		if err != nil {
			return nil, fmt.Errorf("query failed after %v: %w", time.Since(start), err)
		}

		if rerr != nil {
			return nil, fmt.Errorf("query failed: %w", rerr)
		}

		ret = append(ret, res.Items...)
		more = false
		if res.LastEvaluatedKey != nil {
			lastKey = res.LastEvaluatedKey
			more = true
		}

		if input.Limit != nil {
			if int64(len(ret)) >= *input.Limit {
				more = false
				lastKey = nil
			}
		}
	}

	return ret, nil
}

func GetItems(svc *dynamodb.DynamoDB, table, pk, sk string, limit ...int64) ([]map[string]*dynamodb.AttributeValue, error) {
	v1 := strings.Split(pk, ":")
	v2 := strings.Split(sk, ":")
	var input *dynamodb.QueryInput
	if sk != "" {
		skexpr := fmt.Sprintf("%v = :pk AND begins_with(%v, :sk)", v1[0], v2[0])
		input = &dynamodb.QueryInput{
			TableName:              aws.String(table),
			KeyConditionExpression: aws.String(skexpr),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":pk": {S: aws.String(v1[1])},
				":sk": {S: aws.String(v2[1])},
			},
			ScanIndexForward: aws.Bool(false), // descending order
		}

		if len(limit) > 0 {
			input.Limit = aws.Int64(limit[0])
		}
	} else {
		input = &dynamodb.QueryInput{
			TableName:              aws.String(table),
			KeyConditionExpression: aws.String(fmt.Sprintf("%v = :pk", v1[0])),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":pk": {S: aws.String(v1[1])},
			},
			ScanIndexForward: aws.Bool(false), // descending order
		}

		if len(limit) > 0 {
			input.Limit = aws.Int64(limit[0])
		}
	}

	return query(svc, table, input)
}

func GetGsiItems(svc *dynamodb.DynamoDB, table, index, key, value string) ([]map[string]*dynamodb.AttributeValue, error) {
	input := dynamodb.QueryInput{
		TableName:              aws.String(table),
		IndexName:              aws.String(index),
		KeyConditionExpression: aws.String(fmt.Sprintf("%v = :v", key)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": {S: aws.String(value)},
		},
	}

	return query(svc, table, &input)
}

func ScanItems(svc *dynamodb.DynamoDB, table string, limit ...int64) ([]map[string]*dynamodb.AttributeValue, error) {
	start := time.Now()
	ret := []map[string]*dynamodb.AttributeValue{}
	var lastKey map[string]*dynamodb.AttributeValue
	more := true

	in := dynamodb.ScanInput{TableName: aws.String(table)}
	if len(limit) > 0 {
		in.Limit = aws.Int64(limit[0])
	}

	// Could be paginated.
	for more {
		if lastKey != nil {
			in.ExclusiveStartKey = lastKey
		}

		var rerr, err error
		var res *dynamodb.ScanOutput

		// Our retriable, backoff-able function.
		op := func() error {
			res, err = svc.Scan(&in)
			rerr = err
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case dynamodb.ErrCodeProvisionedThroughputExceededException:
						return err // will cause retry with backoff
					}
				}
			}

			return nil // final err is rerr
		}

		err = backoff.Retry(op, backoff.NewExponentialBackOff())
		if err != nil {
			return nil, fmt.Errorf("ScanItems failed after %v: %w", time.Since(start), err)
		}

		if rerr != nil {
			return nil, fmt.Errorf("ScanItems failed: %w", rerr)
		}

		ret = append(ret, res.Items...)
		more = false
		if res.LastEvaluatedKey != nil {
			lastKey = res.LastEvaluatedKey
			more = true
		}

		if in.Limit != nil {
			if int64(len(ret)) >= *in.Limit {
				more = false
				lastKey = nil
			}
		}
	}

	return ret, nil
}

func PutItem(svc *dynamodb.DynamoDB, table string, item map[string]*dynamodb.AttributeValue) error {
	start := time.Now()
	var rerr, err error

	// Our retriable function.
	op := func() error {
		_, err = svc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item:      item,
		})

		rerr = err
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case dynamodb.ErrCodeProvisionedThroughputExceededException:
					return err // will cause retry with backoff
				}
			}
		}

		return nil // final err is rerr
	}

	err = backoff.Retry(op, backoff.NewExponentialBackOff())
	if err != nil {
		return fmt.Errorf("PutItem failed after %v: %w", time.Since(start), err)
	}

	if rerr != nil {
		return fmt.Errorf("PutItem failed: %w", rerr)
	}

	return nil
}

func DeleteItem(svc *dynamodb.DynamoDB, table, pk, sk string) error {
	v1 := strings.Split(pk, ":")
	v2 := strings.Split(sk, ":")
	start := time.Now()
	var input *dynamodb.DeleteItemInput
	if sk == "" {
		input = &dynamodb.DeleteItemInput{
			TableName: aws.String(table),
			Key: map[string]*dynamodb.AttributeValue{
				v1[0]: {S: aws.String(v1[1])},
			},
		}
	} else {
		input = &dynamodb.DeleteItemInput{
			TableName: aws.String(table),
			Key: map[string]*dynamodb.AttributeValue{
				v1[0]: {S: aws.String(v1[1])},
				v2[0]: {S: aws.String(v2[1])},
			},
		}
	}

	var rerr error

	// Our retriable function.
	op := func() error {
		_, err := svc.DeleteItem(input)
		rerr = err
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case dynamodb.ErrCodeProvisionedThroughputExceededException:
					return err // will cause retry with backoff
				}
			}
		}

		return nil // final err is rerr
	}

	err := backoff.Retry(op, backoff.NewExponentialBackOff())
	if err != nil {
		return fmt.Errorf("DeleteItem failed after %v: %w", time.Since(start), err)
	}

	if rerr != nil {
		return fmt.Errorf("DeleteItem failed: %w", rerr)
	}

	return nil
}
