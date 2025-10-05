.PHONY: install fmt lint run doctor

install:
	poetry install

fmt:
	poetry run black src

lint:
	poetry run ruff check src

run:
	poetry run ragchat hello

doctor:
	poetry run ragchat doctor
