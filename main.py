import json
import ijson
import re
import sys
import time
from pprint import pprint
from collections import defaultdict
from mpi4py import MPI

import pandas as pd
import cProfile

STATE_DICT = {'new south wales': 'nsw', 'queensland': 'qld', 'south australia': 'sa', 
              'tasmania': 'tas', 'victoria': 'vic', 'western australia': 'wa', 
              'australian capital territory': 'act', 'northern territory': 'nt'}

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
This function is used to get the sal and gcc dict
'''
def get_sal_gcc_dict(sal_data):
    sal_gcc_dict = {}
    
    for region in sal_data:
        if re.search('\d+(.*)', sal_data[region].get("gcc")).group(1)[0] != 'r' and \
                sal_data[region].get("gcc")[0] != '9':
            if re.findall("\((.*?)\)", region):
                region_name = re.findall(r'(.*?)\(.*?\)', region)
                region_name = region_name[0].strip(" ")
                region_loc = re.findall("\((.*?)\)", region)
                if " - " in region_loc[0]:
                    region_loc = region_loc[0].split(" - ")
                    sal_gcc_dict[region_name, region_loc[0], 
                                region_loc[1].replace(".", "")] = sal_data[region].get("gcc")
                    sal_gcc_dict[region_loc[0], 
                                region_loc[1].replace(".", "")] = sal_data[region].get("gcc")
                    sal_gcc_dict[region_name, 
                                region_loc[1].replace(".", "")] = sal_data[region].get("gcc")
                else:
                    region_loc = region_loc[0].replace(".", "")
                    sal_gcc_dict[region_name, region_loc] = sal_data[region].get("gcc")
            else:
                sal_gcc_dict[region] = sal_data[region].get("gcc")
                
    return sal_gcc_dict

'''
This function is used to get the list of keys for the sal dictionary
'''
def generate_tweet_sal_keys(full_name):
    keys = []
    if full_name[-1] in STATE_DICT.keys():
        for i in range(len(full_name) - 1):
            keys.append(full_name[i])
            keys.append((full_name[i], STATE_DICT[full_name[-1]]))
    else:
        for i in range(len(full_name)):
            keys.append(full_name[i])
    return keys

'''
This function is used to split the full name of the location
'''
def split_full_name(full_name):
    split_names = []
    tmp_split_names = []
    names = full_name.lower().split(",")
    names = [name.strip(" ") for name in names]
    for name in names:
        if " - " in name:
            for split_name in name.split(" - "):
                tmp_split_names.append(split_name)
        else:
            tmp_split_names.append(name)
            
    for name in tmp_split_names:
        if "(" in name:
            name_outside = re.findall(r'(.*?)\(.*?\)', name)[0]
            name_inside = re.findall("\((.*?)\)", name)[0]
            split_names.append(name_outside.strip(" "))
            split_names.append(name_inside.strip("."))
        else:
            split_names.append(name)
            
    return split_names

'''
This function is used to get the number of tweets in each capital city
'''
def get_city_tweet_counts(sal_gcc_dict, tweet, city_tweet_counts):
    full_name = tweet["includes"]["places"][0]["full_name"]
    split_names = split_full_name(full_name)
    tweet_sal_keys = generate_tweet_sal_keys(split_names)
    
    # Ordering the sal keys by tuple with more detailed sal information first, string after
    sorted_tweet_sal_keys = sorted(tweet_sal_keys, key=lambda x: isinstance(x, tuple) != True)

    # Check if the tweet location is in the sal_gcc dictionary;
    # If so, count the number of tweets in each capital city
    for tweet_sal_key in sorted_tweet_sal_keys:
        if tweet_sal_key in sal_gcc_dict.keys():
            if sal_gcc_dict[tweet_sal_key] in city_tweet_counts.keys():
                city_tweet_counts[sal_gcc_dict[tweet_sal_key]] += 1
            else:
                city_tweet_counts[sal_gcc_dict[tweet_sal_key]] = 1
            break

'''
This function is used to get the number of tweets made by each author
'''
def get_author_tweet_counts(tweet, author_tweet_counts):
    author_id = tweet['data']["author_id"]
    if author_id in author_tweet_counts:
        author_tweet_counts[author_id] += 1
    else:
        author_tweet_counts[author_id] = 1
    return author_tweet_counts
        
'''
This function is used to get the number of tweets made by each author in each capital city
'''
def get_author_city_counts(sal_gcc_dict, tweet, author_city_counts):
    
    author_id = tweet['data']["author_id"]
    full_name = tweet["includes"]["places"][0]["full_name"]
    split_names = split_full_name(full_name)
    tweet_sal_keys = generate_tweet_sal_keys(split_names)

    # Ordering the sal keys by tuple with more detailed sal information first, string after
    sorted_tweet_sal_keys = sorted(tweet_sal_keys, key=lambda x: isinstance(x, tuple) != True)

    for tweet_sal_key in sorted_tweet_sal_keys:
        if tweet_sal_key in sal_gcc_dict.keys():
            greater_city = sal_gcc_dict[tweet_sal_key]
            if author_id in author_city_counts:
                if greater_city in author_city_counts[author_id]:
                    author_city_counts[author_id][greater_city] += 1
                else:
                    author_city_counts[author_id][greater_city] = 1
            else:
                author_city_counts[author_id] = {greater_city: 1}
            break 

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
    
    capital_city_lst = []
    sal_gcc_dict = {}
    with open(sal_data, 'r', encoding='utf-8') as f:
        sal_data = json.load(f)
        capital_city_lst = get_capital_cities(sal_data)
        sal_gcc_dict = get_sal_gcc_dict(sal_data)

    sub_city_tweet_counts = {city: 0 for city in capital_city_lst}
    sub_author_tweet_counts = {}
    sub_author_city_counts = {}
    with open(twitter_data, 'rb') as f:
        parser = ijson.parse(f)
        
        count = 0
        for chunk in ijson.items(parser, 'item'):
            if count % size == rank:
                tweet = {'data': chunk['data'], 'includes': chunk['includes']} # keep only wanted data
                
                get_city_tweet_counts(sal_gcc_dict, tweet, sub_city_tweet_counts)
                get_author_tweet_counts(tweet, sub_author_tweet_counts)
                get_author_city_counts(sal_gcc_dict, tweet, sub_author_city_counts)
            count += 1
        
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
    main()
    # cProfile.run("main()", sort="cumtime")