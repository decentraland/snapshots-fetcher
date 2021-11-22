
ifneq ($(CI), true)
LOCAL_ARG = --local --verbose --diagnostics
endif

test:
	CI=true \
		node --trace-warnings node_modules/.bin/jest --detectOpenHandles --coverage --colors --runInBand $(TESTARGS)

test-watch:
	CI=true \
		node --trace-warnings node_modules/.bin/jest --detectOpenHandles --coverage --colors --runInBand --watch $(TESTARGS)

build:
	./node_modules/.bin/tsc -p tsconfig.json
	rm -rf node_modules/@microsoft/api-extractor/node_modules/typescript || true
	./node_modules/.bin/api-extractor run $(LOCAL_ARG) --typescript-compiler-folder ./node_modules/typescript

clean:
	rm downloads/Qm*
	rm downloads/ba*
	rm -rf dist

.PHONY: build test clean
