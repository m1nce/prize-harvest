"""
This script scrapes player projections from the 'Today’s Board' section of the PrizePicks website.
It navigates through the NBA tab, collects player names, projected points, projection types,
and payout types, and exports the collected data to a CSV file named 'players_projections.csv'.

Please ensure that you have Selenium, pandas, and BeautifulSoup installed in your Python environment to run this script.
Also, make sure you have the Chrome WebDriver installed and in your system's PATH.

To run this script, execute the following command in your terminal:
python prizepicks_scrape.py

----------------------------------------------------------------------------------
    By: Minchan Kim
    Version: 1.5
    Last Updated: 2024-03-31
"""

import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import bs4

class PrizePicksScraper:
    """
    A class to scrape player projections from the PrizePicks website.
    """

    def __init__(self):
        """
        Initializes the PrizePicksScraper class.
        """
        self.driver = webdriver.Chrome()
        self.players_projections = pd.DataFrame()


    def scrape(self) -> pd.DataFrame:
        """
        Navigates to the PrizePicks website, interacts with it to find NBA player projections, 
        and collects data into a pandas DataFrame which is stored in self.player_projections.

        This method should be run within the retry method to handle potential transient errors.

        This method closes the WebDriver instance upon completion or error.
        """
        try:
            # Allows Geolocation for browser
            self.driver.execute_cdp_cmd (
                "Browser.grantPermissions",
                {
                    "origin": "https://www.openstreetmap.org/",
                    "permissions": ["geolocation"],
                },
            )
            
            # Opens PrizePicks URL and goes to Today's Board
            self.driver.get('https://www.prizepicks.com/')
            self.driver.find_element(By.LINK_TEXT, 'Today’s Board').click()

            # Lets app.prizepicks.com load, then switches to the tab
            time.sleep(20)
            self.driver.switch_to.window(self.driver.window_handles[-1])

            # Clicks on 'Sounds Good' button to proceed to the site
            self.driver.find_element(By.XPATH, '/html/body/div[3]/div[3]/div/div/div[2]/button').click()
            self.driver.refresh()
            time.sleep(5)

            # Clicks on NBA tab
            self.driver.find_element(By.XPATH, "//div[@class='name' and text()='NBA']").click()
            time.sleep(10)

            # Find all stat elements within the stat-container
            # i.e. categories is the list ['Points','Rebounds',...,'Turnovers']
            categories = self.driver.find_element(By.CSS_SELECTOR, '.stat-container').text.split('\n')
            nbaPlayers = []

            for category in categories:
                # Click on the category to get the player projections
                self.driver.find_element(By.XPATH, f"//div[text()='{category}']").click()
                time.sleep(5)

                # Get the page source and parse it
                html = self.driver.page_source
                soup = bs4.BeautifulSoup(html, 'html.parser')

                # Find all player projections
                projections = soup.find_all('li', {'id': 'test-projection-li'})

                for projection in projections:
                    # get player name, projection, and type of projection
                    name = projection.find('h3', {'id': 'test-player-name'}).text
                    point = projection.find('div', {'class': 'flex flex-1 items-center pr-2'}).text
                    proj_type = projection.find('div', {'class': 'text-soClean-140 max-w-[100px] self-center text-left text-xs leading-[14px]'}).text.strip()
                    team = projection.find('div', {'id': 'test-team-position'}).text.split()[0].strip()
                    opponent = projection.find('time', {'class': 'text-soClean-140 text-xs'}).text.split()[1].strip()

                    # try getting demon/goblin projection
                    try:
                        payout = projection.find('div', {'class': 'absolute -right-4 left-1/2 top-12'}).find('img').get('alt')
                    except AttributeError:
                        payout = 'Standard'

                    # convert point to float
                    point = float(point)

                    # append player data to list
                    players = {'Name': name, 
                               'Prop': point,
                               'Type': proj_type, 
                               'Payout': payout, 
                               'Team': team, 
                               'Opponent': opponent}
                    nbaPlayers.append(players)

            # Updates self.players_projections from the list of player data
            self.players_projections = pd.DataFrame(nbaPlayers)
        finally:
            self.driver.quit()
    

    def retry(self, max_attempts = 5):
        """
        Retries the function if an error occurs.

        Args:
            max_attempts (int): The maximum number of times to retry the function.

        Returns:
            pd.DataFrame: The player projections.
        """
        attempt = 0
        while attempt < max_attempts:
            try:
                self.driver = webdriver.Chrome()
                self.scrape()
                return self.players_projections
            except Exception as e:
                print('An error occurred. Retrying...')
                time.sleep(5)
                attempt += 1
        print('Failed to scrape data after maximum attempts.')
        return None
        
    def save_to_csv(self, filename):
        """
        Saves the player projections to a CSV file.
        
        Args:
            filename (str): The name of the CSV file to save the data to.
        """
        self.players_projections.to_csv(filename, index=False)

if __name__ == '__main__':
    # Create an instance of the PrizePicksScraper class.
    scraper = PrizePicksScraper()

    # Attempt to scrape player projections and save to CSV file.
    data = scraper.retry()

    if data is not None:
        scraper.save_to_csv('players_projections.csv')
        print("Data scraped and saved to players_projections.csv successfully.")