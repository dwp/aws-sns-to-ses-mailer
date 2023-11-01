# DO NOT USE THIS REPO - MIGRATED TO GITLAB

# aws-sns-to-ses-mailer
AWS Lambda application to send emails via AWS SES using information recieved from AWS SNS notification

## Usage

This lambda is used in DataWorks to send out emails when needed. It is not used for any daily BAU purposes. The lambda is deployed via the `aws-common-infrastructure` repository.

### Templates

The lambda uses templates, which are deployed from the `aws-common-infrastructure` repository as well.

### Sending emails

In order to send an email from this lambda, you can invoke it with a payload that looks like an sns message:

    {
      "Records": [
        {
          "Sns": {
            "Message": {
            ...
            }
          }
        }
      ]
    }

The message object should have the following properties:

* `recipients` -> A `;` separated list of email recipients
* `mailing_list` -> A mailing list name for the email to go to
* `bucket` -> The S3 bucket name containing the email templates
* `plain_text_template` -> The name of the plain text email template file from s3
* `template_variables` -> The variables as a key/value pair dictionary that with be inserted in to the template
* `html_template` -> The name of the html email template file from s3
* `from_local_part` -> The name of the from field
