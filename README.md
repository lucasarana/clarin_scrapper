# clarin_scrapper
Scrapper for public CVs on Clarin-Empleos


Prerequisities:

- Postgres database (or a db that supports multiple threads/connections)
- Clarin-Empleos User

Requirements:
. requests
. BeautifulSoup
. bs4

What is it?

Scrapps in multiple threads for or public CVs and saves them in a postgres database. It currently only saves images, name, email, phones and addresses. Still CV missing.
