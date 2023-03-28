import json
import ijson
import re
import sys
import time
from collections import defaultdict
from itertools import islice
from mpi4py import MPI

import pandas as pd

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
                greater_city = sal_city_match[sub_name]
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
        
    # # to make a different sample for testing
    # author_city_counts['940868397528698880']['2gmel'] = 5
    # author_city_counts['940868397528698880']['3gbri'] = 2
    # author_city_counts['7050962']['3gbri'] = 10
    
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

if __name__=="__main__": 
    
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

    # sal_data='sal.json'
    # twitter_data='tinyTwitter.json'
    
    # Open the JSON file
    if rank == 0:
        start = time.time()
        
        with open(twitter_data, 'rb') as f:
            objects = ijson.items(f, 'item', use_float=True)      
            objects = list(objects)
            
            # Calculate the number of items in each subgroup
            subgroup_size = len(objects) // size

            # Divide the objects into subgroups
            subgroups = [objects[i:i+subgroup_size] for i in range(0, len(objects), subgroup_size)]
            
            # If the number of subgroups is greater than the number of processes, merge the last two subgroups
            if len(subgroups) > size and len(subgroups[-1]) < subgroup_size:
                subgroups[-2].extend(subgroups[-1])
                subgroups.pop()
    else:
        subgroups = None
        
    with open(sal_data) as f:
        sal_data = json.load(f)

    # Scatter the subgroups to different processes
    subgroup = comm.scatter(subgroups, root=0)

    # Process the data
    sub_city_tweet_counts, sub_author_tweet_counts, sub_author_city_counts = data_process(sal_data, subgroup)
    
    # Gather the sub-results
    city_tweet_counts_gather = comm.gather(sub_city_tweet_counts, root=0)
    author_tweet_counts_gather = comm.gather(sub_author_tweet_counts, root=0)
    author_city_counts_gather = comm.gather(sub_author_city_counts, root=0)

    # Output the results
    if rank == 0:
        city_tweets_df, top_ten_df, author_city_counts_df = output(city_tweet_counts_gather, author_tweet_counts_gather, author_city_counts_gather)
        
        # city_tweets_df.to_csv("./output/city_tweets.csv") 
        # top_ten_df.to_csv("./output/ctop_ten.csv")
        # author_city_counts_df.to_csv("./output/cauthor_city_counts.csv")
        
        print(city_tweets_df)
        print("\n", "*"*80, "\n")
        print(top_ten_df)
        print("\n", "*"*80, "\n")
        print(author_city_counts_df)
        
        print("\nTime taken: ", time.time() - start)
