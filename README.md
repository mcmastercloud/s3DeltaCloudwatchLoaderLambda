# Introduction
This function receives a notification from EventBridge when a file is added to an S3 Bucket.

This is useful in cases where an AWS Service write to S3, but it unable to write to CloudWatch.

<strong>Note:</strong> the s3 Bucket must have versioning enabled.

The function then:
	- Looks at the file (or a version of the file)
	- If the file is the first entry (i.e. not a version), then the file is read in its entirety
	- If the file is a version of an existing file, then the file is compared to the last version of the file, and a Delta is taken
	- For ever record in either the file, or the delta, a record is written to cloudwatch.


# Using this code in a Lambda
Set the following environment variables:
	- FILE_SEPARATOR = Comma delimited list of ASCII Character code to use as a line separator (i.e. 10 = \n)
	- CLOUDWATCH_LOG_GROUP = The Name of the Log Group that the records will be written to

## Notes
- Has not been tested on a massive scale; since the intent is to use this for MSSQL Audit Logs, and the Log Size will be set to 2Mb; load testing was not performed beyond this.
