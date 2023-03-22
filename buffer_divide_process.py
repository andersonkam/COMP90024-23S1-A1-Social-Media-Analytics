import json
import ijson
import re
import sys
import time
from collections import defaultdict
from itertools import islice
from mpi4py import MPI

import pandas as pd
import cProfile

BUFFER_SIZE = 1000

'''
This function is used to get the list of capital cities required
'''
def get_capital_cities(sal_data):
    capital_city_lst = []

    for k in sal_data.keys():
        gcc = sal_data[k]["gcc"]
        # get rid of rural locations
        if gcc[1] != "r" and gcc not in capital_city_lst:
            capital_city_lst.append(gcc)

    return capital_city_lst

'''
This function is used to get the match between the sal and the capital city
'''
def get_sal_city_match(sal_data):
    sal_city_match = {}
    capital_cities = get_capital_cities(sal_data)

    for region in sal_data.keys():
        gcc = sal_data[region]["gcc"]

        if gcc in capital_cities:
            sal_city_match[region] = gcc

    return sal_city_match

'''
This function is used to get a list of subname of the full name of the location
'''
def get_sal_name(full_name):
    sal_names = full_name.split(",")[0:]  # split by comma and take the first part
    sal_names = [name.strip().lower() for name in sal_names]
    
    if len(sal_names) > 1:
        sub_names = []
        for name in sal_names[1:]:
            sal_names.extend(re.split(r"\W+", name))
        sal_names.extend([name for name in sub_names if name])
        
    return sal_names

'''
This function is used to get the number of tweets in each capital city
'''
def get_city_tweet_counts(sal_data, twitter_data):
    capital_city_lst = get_capital_cities(sal_data)
    sal_city_match = get_sal_city_match(sal_data)

    city_tweet_counts = {city: 0 for city in capital_city_lst}

    for tweet in twitter_data:
        if tweet["includes"] and "places" in tweet["includes"]:
            full_name = tweet["includes"]["places"][0]["full_name"]
            
            if full_name in sal_city_match.keys():
                greater_city = sal_city_match[sub_name]
                author_id = tweet['data']["author_id"]
            else:  
                sal_name = get_sal_name(full_name)

                for sub_name in sal_name:
                    if sub_name in sal_city_match.keys():
                        greater_city = sal_city_match[sub_name]
                        city_tweet_counts[greater_city] += 1
                        break

    return city_tweet_counts

'''
This function is used to get the number of tweets made by each author
'''
def get_author_tweet_counts(twitter_data):
    author_tweet_counts = {}
    for tweet in twitter_data:
        author_id = tweet['data']["author_id"]
        if author_id in author_tweet_counts:
            author_tweet_counts[author_id] += 1
        else:
            author_tweet_counts[author_id] = 1

    return author_tweet_counts

'''
This function is used to get the number of tweets made by each author in each capital city
'''
def get_author_city_counts(sal_data, twitter_data):
    author_city_counts = {}
    sal_city_match = get_sal_city_match(sal_data)

    for tweet in twitter_data:
        if tweet["includes"] and "places" in tweet["includes"]:
            full_name = tweet["includes"]["places"][0]["full_name"]
            
            if full_name in sal_city_match.keys():
                greater_city = sal_city_match[full_name]
                author_id = tweet['data']["author_id"]
            else:            
                sal_name = get_sal_name(full_name)

                for sub_name in sal_name:
                    if sub_name in sal_city_match.keys():
                        greater_city = sal_city_match[sub_name]
                        author_id = tweet['data']["author_id"]
                        if author_id in author_city_counts:
                            if greater_city in author_city_counts[author_id]:
                                author_city_counts[author_id][greater_city] += 1
                            else:
                                author_city_counts[author_id][greater_city] = 1
                        else:
                            author_city_counts[author_id] = {greater_city: 1}
                        break 
                
    return author_city_counts
    
'''
This function is used to process the data into desired format
'''
def data_process(sal_data, twitter_data):
    city_tweet_counts = get_city_tweet_counts(sal_data, twitter_data)
    author_tweet_counts = get_author_tweet_counts(twitter_data)
    author_city_counts = get_author_city_counts(sal_data, twitter_data)
    
    return city_tweet_counts, author_tweet_counts, author_city_counts

'''
This function is used to process the gathered data and
output the results of the analysis in dataframe format
'''
def output(city_tweet_counts_gather, author_tweet_counts_gather, author_city_counts_gather):
    city_tweet_counts = defaultdict(int)
    author_tweet_counts = {}
    author_city_counts = {}
    
    for lst in city_tweet_counts_gather:
        for key, value in lst.items():
            city_tweet_counts[key] += value
            
    for lst in author_tweet_counts_gather:
        for key, value in lst.items():
            if key in author_tweet_counts:
                author_tweet_counts[key] += value
            else:
                author_tweet_counts[key] = value
                
    for lst in author_city_counts_gather:
        for key, value in lst.items():
            if key in author_city_counts:
                for inner_key, inner_value in value.items():
                    if inner_key in author_city_counts[key]:
                        author_city_counts[key][inner_key] += inner_value
                    else:
                        author_city_counts[key][inner_key] = inner_value
            else:
                author_city_counts[key] = value
    
    # Output of tweets number in each captial city
    city_tweets_df = pd.DataFrame.from_dict(
        city_tweet_counts, orient="index", columns=["Number of Tweets Made"]
    ).rename_axis("Greater Capital City")
    
    # Output of top ten authors with most numebr of tweets
    top_ten = sorted(author_tweet_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    top_ten_df = pd.DataFrame(
        top_ten, columns=["Author Id", "Number of Tweets Made"]
    ).rename_axis("Rank")
    top_ten_df.index += 1
    
    # Sort the dict by the unique number of cities
    author_city_counts = sorted(author_city_counts.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    rows = []

    for i, (author_id, city_tweet_counts) in enumerate(author_city_counts):
        num_unique_cities = len(city_tweet_counts)
        num_tweets = sum(city_tweet_counts.values())
        row = {
            'Author Id': author_id,
            'Number of Unique City Locations': num_unique_cities,
            '#Tweets': f"#{num_tweets} tweets - {', '.join(f'{count}{city[1:]}' for city, count in city_tweet_counts.items())}"
        }
        rows.append(row)
        
    author_city_counts_df = pd.DataFrame(rows).rename_axis("Rank")
    author_city_counts_df.index += 1
    
    return city_tweets_df, top_ten_df, author_city_counts_df

'''
This function is used to update the sub results from new buffer results
'''
def update_sub_results(sub_city_tweet_counts, sub_author_tweet_counts, sub_author_city_counts, \
                        buffer_city_tweet_counts, buffer_author_tweet_counts, buffer_author_city_counts):
    # Update city tweet counts
    for city, count in buffer_city_tweet_counts.items():
        if city in sub_city_tweet_counts:
            sub_city_tweet_counts[city] += count
        else:
            sub_city_tweet_counts[city] = count

    # Update author tweet counts
    for author, count in buffer_author_tweet_counts.items():
        if author in sub_author_tweet_counts:
            sub_author_tweet_counts[author] += count
        else:
            sub_author_tweet_counts[author] = count

    # Update author city counts
    for author, city_count in buffer_author_city_counts.items():
        if author in sub_author_city_counts:
            for city, count in city_count.items():
                if city in sub_author_city_counts[author]:
                    sub_author_city_counts[author][city] += count
                else:
                    sub_author_city_counts[author][city] = count
        else:
            sub_author_city_counts[author] = city_count

def main():
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()
    
    # Check if the correct number of command line arguments are provided
    if len(sys.argv) != 3:
        print("Usage: python filename.py <sal_data> <twitter_data>")
        sys.exit(1)
    else:
        # Retrieve the command line arguments
        sal_data = sys.argv[1]
        twitter_data = sys.argv[2]

    if rank == 0:
        start = time.time()
        print("Reading data...")

    with open(sal_data) as f:
        sal_data = json.load(f)
    
    sub_city_tweet_counts = defaultdict(int)
    sub_author_tweet_counts = {}
    sub_author_city_counts = {}
    
    tweet_buffer = []
    with open(twitter_data, 'rb') as f:
        parser = ijson.parse(f)
        
        count = 0
        for chunk in ijson.items(parser, 'item'):
            # Only process the data if the current process is the one that should process it
            if count // BUFFER_SIZE % size == rank:
                tweet = {'data': chunk['data'], 'includes': chunk['includes']} # keep only wanted data
                                
                tweet_buffer.append(tweet)
                if len(tweet_buffer) == BUFFER_SIZE:
                    
                    # Process the data for the current buffer
                    buffer_city_tweet_counts, buffer_author_tweet_counts, \
                                    buffer_author_city_counts = data_process(sal_data, tweet_buffer)
                    
                    # Update the sub results
                    update_sub_results(sub_city_tweet_counts, sub_author_tweet_counts, sub_author_city_counts, \
                            buffer_city_tweet_counts, buffer_author_tweet_counts, buffer_author_city_counts)

                    # Clear the buffer
                    tweet_buffer = []
            count += 1

        # Process remaining data in buffer in process 0 or send to other processes
        if tweet_buffer:            
            buffer_city_tweet_counts, buffer_author_tweet_counts, \
                            buffer_author_city_counts = data_process(sal_data, tweet_buffer)
            
            # Update the sub results
            update_sub_results(sub_city_tweet_counts, sub_author_tweet_counts, sub_author_city_counts, \
                    buffer_city_tweet_counts, buffer_author_tweet_counts, buffer_author_city_counts)
    
    # Gather the sub-results
    city_tweet_counts_gather = comm.gather(sub_city_tweet_counts, root=0)
    author_tweet_counts_gather = comm.gather(sub_author_tweet_counts, root=0)
    author_city_counts_gather = comm.gather(sub_author_city_counts, root=0)

    if rank == 0:
        city_tweets_df, top_ten_df, author_city_counts_df = output(city_tweet_counts_gather, author_tweet_counts_gather, author_city_counts_gather)
        
        # city_tweets_df.to_csv("./output/city_tweets.csv") 
        # top_ten_df.to_csv("./output/top_ten.csv")
        # author_city_counts_df.to_csv("./output/cauthor_city_counts.csv")
        
        pd.set_option('display.max_columns', None)
        
        print(city_tweets_df)
        print("\n", "*"*80, "\n")
        print(top_ten_df)
        print("\n", "*"*80, "\n")
        print(author_city_counts_df)

        print("Process 0: Finished in {:.2f} seconds".format(time.time() - start))

if __name__=="__main__": 
    # cProfile.run("main()", sort="cumtime")
    main()