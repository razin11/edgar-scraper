# edgar_scraper

ABOUT THE PROJECT

The project is an attempt to scrape financial data from the edgar database to build an in-house database for financial analysis and build fundamental factor-driven quant models. Instead of paying a subscription to create, run and backtest your models, you can have your own proprietary database to do all of the following tasks. The project includes three python files (setup.py, wikitable_scraper.py and edgar_scraper_xbrl.py) and requirements.txt file. 

HOW THE SCRAPER WORKS

The data scraped from financial statements is imported to the database created on a local server as a result a database server/engine is required. I recommend using MySQL since the programs are written in compliance with MySQL. A metadata table (symbol) is created in the database file, which contains all S&P500 companies. When running the scraper company info (id, cik and ticker) is extracted from the symbol table and each company is run in a loop, scraping the data from financial statements and appending it to the database. Note that new companies could easily be added to the metadata (symbol) table beyond S&P500 companies to enrich the overall database. 

When running the scraper, change IDX and COMPANIES_TO_RUN variables to indicate which companies you want to run the scraper for. Look for the id column in the symbol table to match the id representing the company. For example, if you want to start from the 7th company in the database and want to run the following 10 companies, then IDX = 6 (since first company id is zero) and COMPANIES_TO_RUN = 10. Also, note that measures have been taken to avoid duplication in the database. IDX and COMPANIES_TO_RUN are args for edgar_crawler function, which acts as the main function.

The current scraper is quite slow right now since a lot of variation is tested for every company to identify and scrape the correct data. Currently working to reduce the run-time for every filing. 

RELATED PROJECTS

I am also working on several other projects, which will be uploaded soon. Highly complementary to this repo will be a security price repo. Combining these two repos I will be providing a repo for creating factor models, which will include creation of security baskets based on a ranking system and optimization models to assign weights to securities, all of which will depend on the databases created using edgar-scraper and security-prices repos. Check out the security-price repo (https://github.com/razin11/security-prices). 

