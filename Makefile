update-pip:
	pip install --upgrade pip

init: update-pip
	pip install -r requirements.txt
