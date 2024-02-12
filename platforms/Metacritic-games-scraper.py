"""
Introduction to Metacritic Game Data Scraping Project:

This project is designed to collect comprehensive video game data from Metacritic, a popular review aggregation website for games, movies, and more. 
Given the lack of an official API for accessing Metacritic's vast database, this part of the project focuses on developing a custom web scraper to systematically gather information about video games.
My goal is to compile data across various platforms, including game titles, release dates, Metascores, user scores, and descriptions, among other relevant details.

The data collected through this scraping process will serve as a foundation for in-depth analysis aimed at uncovering trends in game ratings,
comparing user scores with critic scores, and identifying factors that might influence the success and reception of video games across different genres and platforms.
By analyzing this data, we hope to gain insights into the gaming industry's dynamics, such as the impact of release timing on game reception, the correlation between game scores and their popularity, and trends in genre preferences over time.

This script is structured to navigate through Metacritic's game listings, handling pagination and extracting the required information from each game's detail page. 
Special attention has been given to respectful web scraping practices, adhering to Metacritic's robots.txt file and incorporating delays between requests to avoid overloading their servers. 
The resulting dataset, saved as 'metacritic_games_data.csv', will be utilized in subsequent parts of the project for exploratory data analysis (EDA) and visualization to share our findings and insights into the gaming industry.

Please note, this script is part of a larger project that includes data cleaning, analysis, and visualization components. 
Each part is designed to build upon the data and insights gathered in the previous stages, culminating in a comprehensive analysis of video game ratings and trends on Metacritic.
"""

# %%
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import re

# %%
# Headers to mimic a browser visit
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

# %%
def fetch_and_parse(url):
    """
    Fetches the HTML content of a webpage specified by its URL and parses it into a BeautifulSoup object.
    
    Args:
    - url (str): The URL of the webpage to fetch and parse.
    
    Returns:
    - BeautifulSoup object if the page was successfully fetched and parsed; None otherwise.
    
    This function sends a GET request to the specified URL. If the request is successful (HTTP status code 200),
    it uses BeautifulSoup to parse the page content into a structured format for easy manipulation. If the request
    fails, it prints an error message and returns None to indicate failure.
    """
    
    # Send a GET request to the specified URL
    response = requests.get(url, headers=headers)
    
    # Check if the response status code is 200 (OK)
    if response.status_code == 200:
        # Parse the page content using BeautifulSoup and the 'html.parser' parser
        soup = BeautifulSoup(response.content, "html.parser")
        # Return the parsed page as a BeautifulSoup object
        return soup
    else:
        # Print an error message if the page could not be retrieved
        print("Failed to retrieve the page")
        # Return None to indicate the function was unable to fetch and parse the page
        return None


# %%
def extract_game_data(soup):
    """
    Extracts game data from a BeautifulSoup object representing a webpage with game listings.
    
    Args:
    - soup (BeautifulSoup): BeautifulSoup object of a webpage.
    
    Returns:
    - list of dicts: Each dict contains data about a game, including its title, release date,
      rating, description, and metascore.
      
    This function iterates over each game listing found on the page, extracting relevant information
    and handling cases where specific data might be missing or formatted differently.
    """

    # Initialize a list to store data for each game
    games_data = []

    # Find all game listing containers on the page
    game_listings = soup.find_all("a", class_="c-finderProductCard_container")

    # Iterate over each game listing to extract information
    for game in game_listings:
        # Extract the game title, handling titles with leading numbers and commas
        title_section = game.find("div", class_="c-finderProductCard_title")
        title = (
            re.sub(
                r"^\d{1,3}(,\d{3})*\.", "", title_section.get_text(strip=True)
            ).strip()
            if title_section
            else "Title not found"
        )

        # Initialize variables for release date and rating
        release_date = "Unknown"
        rating = "Rating not found"

        # Extract meta information section for additional details
        meta_info_section = game.find("div", class_="c-finderProductCard_meta")
        if meta_info_section:
            # Parse meta information to extract release date and rating
            meta_info = meta_info_section.get_text(strip=True, separator="|").split("|")
            try:
                # Extract and format the release date
                release_date_str = [info.strip() for info in meta_info if "," in info][0]
                release_date = datetime.strptime(
                    release_date_str, "%b %d, %Y"
                ).strftime("%Y-%m-%d")
            except (ValueError, IndexError):
                # If extraction fails, keep release_date as "Unknown"
                pass
            
            # Extract the rating, looking for a pattern matching "Rated [Rating]"
            rating_text = meta_info_section.get_text()
            match = re.search(r"Rated\s+(\w+)", rating_text)
            if match:
                rating = match.group(1)

        # Extract the game description, handling cases where it might be missing
        description_section = game.find("div", class_="c-finderProductCard_description")
        description = (
            description_section.get_text(strip=True)
            if description_section
            else "Description not found"
        )

        # Extract the metascore, handling cases where it might not be available
        metascore_section = game.find("div", class_="c-siteReviewScore")
        metascore = (
            metascore_section.get_text(strip=True)
            if metascore_section
            else "Metascore not available"
        )

        # Append extracted data for the current game to the list of games data
        games_data.append(
            {
                "title": title,
                "release_date": release_date,
                "rating": rating,
                "description": description,
                "metascore": metascore,
            }
        )

    # Return the list of extracted game data
    return games_data


# %%
# Initialize an empty list to hold all games data
all_games_data = []

# Define the base URL template for scraping, including placeholders for year range and page number
base_url = "https://www.metacritic.com/browse/game/?releaseYearMin=1958&releaseYearMax=2024&page={}"

# Start from the first page
page = 1

# Use an infinite loop to iterate through all pages
while True:
    # Format the current page's URL
    url = base_url.format(page)
    print(f"Scraping {url}")
    
    # Fetch and parse the HTML content of the current page
    soup = fetch_and_parse(url)
    
    # If fetching the page fails, stop the loop
    if not soup:
        print(f"Failed to retrieve page {page}. Exiting loop.")
        break

    # Find all game listings on the current page
    game_listings = soup.find_all("a", class_="c-finderProductCard_container")
    
    # If no game listings are found, assume we've reached the last page and exit the loop
    if not game_listings:
        print(f"No game listings found on page {page}. Assuming end of pages and exiting loop.")
        break

    # Extract data for all games found on the current page
    page_data = extract_game_data(soup)
    
    # Add the extracted data to the cumulative list of all games data
    all_games_data.extend(page_data)

    # Prepare to process the next page
    page += 1

# After collecting data from all pages, create a DataFrame from the accumulated game data
df = pd.DataFrame(all_games_data)

# Print the first few rows of the DataFrame to verify the collected data
print(df.head())

# Optionally, save the collected game data to a CSV file for further use or analysis
df.to_csv("metacritic_games_data.csv", index=False)


# %%
# List of gaming platfroms available in the Metacritic filters
platforms = [
    "ps5",
    "xbox-series-x",
    "nintendo-switch",
    "pc",
    "mobile",
    "3ds",
    "dreamcast",
    "ds",
    "gba",
    "gamecube",
    "meta-quest",
    "nintendo-64",
    "ps1",
    "ps2",
    "ps3",
    "ps4",
    "psp",
    "ps-vita",
    "wii",
    "wii-u",
    "xbox",
    "xbox-360",
    "xbox-one",
]

# %%
def create_dfs(names):
    """
    Creates a dictionary of empty pandas DataFrames for each name provided.
    
    Args:
    - names (list of str): A list of names for which to create empty DataFrames.
    
    Returns:
    - dict: A dictionary where each key is a name from the input list and each value is an empty DataFrame.
    """
    
    # Initialize an empty dictionary to hold the DataFrames
    dfs = {}
    
    # Iterate over each name in the provided list
    for x in names:
        # Create an empty DataFrame for each name and add it to the dictionary
        dfs[x] = pd.DataFrame()
    
    # Return the dictionary containing the empty DataFrames
    return dfs

# Example usage of the function to create a dictionary of DataFrames for different platforms
platforms_dfs = create_dfs(platforms)


# %%
# Define the base URL template for scraping Metacritic game listings.
# The URL includes placeholders for platform and pagination, allowing for dynamic URL construction.
base_url = "https://www.metacritic.com/browse/game/{}/all/all-time/metascore/?releaseYearMin=1958&releaseYearMax=2024&platform={}&page={}"

# Iterate over a list of gaming platforms to scrape game data for each platform.
for platform in platforms:
    all_games_data = []  # Initialize a list to store data for all games on the current platform.
    page = 1  # Start scraping from the first page of game listings.

    # Use a while loop to continuously scrape pages until no more data can be retrieved.
    while True:
        # Construct the URL for the current page of the platform's game listings.
        url = base_url.format(platform, platform, page)
        print(f"Scraping {url}")  # Log the URL being scraped to monitor progress.

        # Attempt to fetch and parse the HTML content of the page.
        soup = fetch_and_parse(url)
        
        # Check if the page was successfully fetched; if not, exit the loop for this platform.
        if not soup:
            print(f"Failed to retrieve page {page}. Exiting loop.")
            break

        # Search the parsed HTML for game listings using their HTML class identifier.
        # This step is crucial for identifying the relevant data to extract.
        game_listings = soup.find_all("a", class_="c-finderProductCard_container")
        
        # If no game listings are found, it's likely that there are no more pages of data, so exit the loop.
        if not game_listings:
            print(f"No game listings found on page {page}. Assuming end of pages and exiting loop.")
            break

        # Extract data for all games found on the current page.
        page_data = extract_game_data(soup)
        # Append the extracted data to the list holding data for all games on the current platform.
        all_games_data.extend(page_data)

        page += 1  # Prepare to scrape the next page by incrementing the page counter.

    # After collecting data for all pages, store the data in a DataFrame keyed by the platform.
    platforms_dfs[platform] = pd.DataFrame(all_games_data)
    # Save the DataFrame to a CSV file, naming it according to the platform for easy identification.
    platforms_dfs[platform].to_csv("platforms/metacritic_games_data_" + platform + ".csv", index=False)

# %%
# Function to merge platform dataframes into the main dataframe
def merge_platform_dfs(main_df, platform_dfs):
    """
    Merges platform-specific data into the main dataframe, appending platform names and scores to each game.
    
    Args:
    - main_df (pd.DataFrame): The main dataframe containing aggregated game data.
    - platform_dfs (dict): A dictionary of dataframes, each representing game data for a specific platform.
    
    Returns:
    - pd.DataFrame: The updated main dataframe with platform and platform score information added.
    
    This function iterates through each platform-specific dataframe, matching games based on the title.
    For each match found, it appends the platform name and the game's metascore on that platform to lists
    in the 'platforms' and 'platform_scores' columns, respectively, of the main dataframe.
    """
    
    # Initialize columns in the main dataframe for storing platform names and scores
    main_df["platforms"] = [[] for _ in range(len(main_df))]
    main_df["platform_scores"] = [[] for _ in range(len(main_df))]
    
    # Iterate over each platform and its corresponding dataframe
    for platform, platform_df in platform_dfs.items():
        # Iterate over each game in the platform-specific dataframe
        for idx, platform_row in platform_df.iterrows():
            # Find indices in the main dataframe that match the current game's title
            matched_idx = main_df[(main_df["title"] == platform_row["title"])].index
            
            # For each match found, append platform and metascore to the corresponding lists in the main dataframe
            for i in matched_idx:
                # Avoid duplicating platform information for a game
                if platform not in main_df.at[i, "platforms"]:
                    main_df.at[i, "platforms"].append(platform)
                    main_df.at[i, "platform_scores"].append(platform_row["metascore"])

    # Return the updated main dataframe
    return main_df


# %%
# Start with an initial dataframe 'df' that may contain pre-existing game data or could be a template for the data to be merged.
all_games_df = df

# Merge the individual platform dataframes stored in 'platforms_dfs' into the 'all_games_df'.
# The 'merge_platform_dfs' function presumably takes the initial dataframe and a dictionary of platform-specific dataframes,
# combining them into a single comprehensive dataframe that includes game data across all platforms.
all_games_df = merge_platform_dfs(all_games_df, platforms_dfs)

# After merging, save the consolidated game data to a CSV file.
# This step exports the combined dataset to 'metacritic_games_data.csv', providing a unified view of the data.
# The 'index=False' parameter ensures that the dataframe index is not included in the CSV, only the data.
all_games_df.to_csv("metacritic_games_data.csv", index=False)



