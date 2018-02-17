init:
	pip install -r requirements.txt

test:
	nosetests tests

freeze: 
	pip freeze > requirements.txt
