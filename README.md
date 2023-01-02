# Pipeline_project

Project done to train pipeline skills.

The pipeline script runs the extract, transform and load of data mined from three newsites into a database

- EL Universal (Mexican media) [Click here to go to "El Universal"](https://www.eluniversal.com.mx/)
- Portopt (Portuguese Media) [Click here to go to "Porto PT"](https://www.porto.pt/pt)
- Portugal News (Portuguese Media) [Click here to go to "The Portugal News"](https://www."theportugalnews.com/)
"
## Information Extracted

From each newsite, the following information was extracted:
- URL
- Host 
- Title  
- Article 
- Number of keywords in titled
- Number of keywords in body

## How to run it

Run the script **pipeline.py** with python3 from the console line.

The output will be a database in the load folder.

## Take a peak of the data

If curious, please check the csv files in the load directory where you can view the structure of the csv used in the process.

## You want to modify the newsite

Change the config.yaml file.

1. Change news_site id
2. Change url
3. change the queries to select title, and body using html xpath.

## Licence

MIT