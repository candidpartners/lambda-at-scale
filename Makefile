OUT=out

NOW=$(shell date +%Y%m%d%H%M%S)
FXN_NAME?=cc-$(NOW)
S3_BUCKET?=candid-serverlessrepo
OUTPUT_CF=$(OUT)/serverless.yaml
REGION?=us-east-1
APPLICATION_NAME=LambdaScale
APPLICATION_ID=arn:aws:serverlessrepo:$(REGION):$(AWS_ACN):applications/$(APPLICATION_NAME)
VERSION?=1.0.$(NOW)

INDEX_ZIP = $(OUT)/index.zip
STAMP_SETUP = $(OUT)/stamp-setup

AWS=aws --profile $(AWS_PROFILE)

.DEFAULT_GOAL := $(OUTPUT_CF)

$(STAMP_SETUP): | $(OUT)
	npm i --prefix $(OUT) aws-sdk

$(INDEX_ZIP): index.js tags | $(OUT)
	nodejs -c $<
	zip $@ $<

upgrade: $(INDEX_ZIP) $(STAMP_SETUP)
	$(AWS) lambda update-function-code \
		--zip-file fileb://$< \
		--function-name $(FXN_NAME)

clean:
	rm -rf $(OUT)

$(OUTPUT_CF): crawl.yaml index.js | $(OUT)
	sam package \
		--template-file $< \
		--output-template-file $(OUTPUT_CF) \
		--s3-bucket $(S3_BUCKET)

package: $(OUTPUT_CF)

deploy-cf: $(OUTPUT_CF)
	aws cloudformation deploy --template-file $< --stack-name $(FXN_NAME) --capabilities CAPABILITY_IAM

deploy: CONF_YAML=$(APPLICATION_NAME)-$(VERSION).yaml
deploy: $(OUTPUT_CF)
	$(AWS) s3 cp $< s3://$(S3_BUCKET)/$(CONF_YAML)
	$(AWS) serverlessrepo create-application-version \
		--application-id $(APPLICATION_ID) \
		--semantic-version $(VERSION) \
		--source-code-url https://github.com/candidpartners/lambda-at-scale \
		--template-url s3://$(S3_BUCKET)/$(CONF_YAML)
	$(AWS) s3 rm s3://$(S3_BUCKET)/$(CONF_YAML)

destroy:
	$(AWS) cloudformation delete-stack --stack-name $(FXN_NAME)

test: | $(OUT)
	$(AWS) lambda invoke --function-name $(FXN_NAME) --invocation-type Event $(OUT)/test.$(shell date +%s)

tags: index.js
	ctags --recurse=yes .

$(OUT):
	mkdir -p $@

.PHONY: setup push clean package deploy test deploy-cf
