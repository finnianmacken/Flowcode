import pandas as pd
import requests
import re
import os


def generate_flowcodes(client_id,
                       id_column_name,
                       campaign_column_name,
                       redirect_url,
                       dataset,
                       smart_rules={},
                       parent_dir="",
                       pass_id_as_argument=True):
    """
    Generates a set of flowcodes from a database, saving them as local svg
    files.

    Parameters
    ----------
    client_id: str
        unique identifier used to authenticate users for the API
    id_column_name: str
        the name of the column containing object ids in the dataset
    campaign_column_name: str
        the name of the column containing campaign ids in the dataset
    redirect_url: str
        the url that the flowcodes will redirect to
    dataset: pd.Dataframe
        dataset of object and campaign ids; must be a pandas DataFrame
    smart_rules: dict
        a smart rules object, of the form specified in the documentation;
        if left blank, will be ignored
    parent_dir: str
        the path to the directory where the svg files will be saved; if blank,
        will use the current working directory
    pass_id_as_argument: bool
        whether to create unique links to redirect_url for each flowcode using
        their ids as url arguments

    Outputs
    -------
    None

    Side Effects
    ------------
    Saves a folder containing all generated flowcodes into the specified
    directory, creating all associated urls and campaigns
    """

    # STEP ZERO: ERROR CHECKING
    _error_checking(client_id, dataset, id_column_name, campaign_column_name)

    # setting parent directory
    if not parent_dir:
        parent_dir = os.getcwd()

    dataset = dataset.loc[:, [id_column_name, campaign_column_name]]
    campaigns = dataset[campaign_column_name].unique()

    # STEP ONE: GENERATING CAMPAIGNS
    _generate_campaigns(campaigns=campaigns,
                        client_id=client_id,
                        id_column_name=id_column_name,
                        campaign_column_name=campaign_column_name,
                        dataset=dataset,
                        reserved_urls=False)

    # STEP TWO: PRE-PROCESSING URLS
    flowcodes = _preprocess_urls(campaigns=campaigns,
                                 id_column_name=id_column_name,
                                 campaign_column_name=campaign_column_name,
                                 redirect_url=redirect_url,
                                 dataset=dataset,
                                 pass_id_as_argument=pass_id_as_argument)

    # STEP THREE: SENDING URL POST REQUESTS FOR EACH CAMPAIGN
    responses = _generate_urls(flowcodes=flowcodes,
                               campaigns=campaigns,
                               client_id=client_id,
                               smart_rules=smart_rules)

    # STEP FOUR: ACCESSING QR CODE REQUESTS
    generated_urls = _process_url_responses(responses)

    # STEP FIVE: CREATING SVGS
    _generate_svgs(parent_dir=parent_dir,
                   generated_urls=generated_urls)

    return "Flowcodes Generated"


def _error_checking(client_id, dataset, id_column_name, campaign_column_name):
    # checking client_id
    regex = "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}"

    if not isinstance(client_id, str):
        raise Exception(f"The client id must be a string, but is a: {type(client_id)}")

    if not re.fullmatch(regex, client_id):
        raise Exception("Your client id doesn't look like it's the right format"
                        "(aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)")

    # checking if we're using a pandas dataframe
    if not isinstance(dataset, pd.DataFrame):
        raise Exception("The dataset argument must be a Pandas Dataframe")

    if id_column_name not in dataset.columns:
        raise Exception("The id column is not specified correctly. \n"
                        "Make sure it's a name, not an index!")

    # checking if the columns exist in the dataframe
    if campaign_column_name not in dataset.columns:
        raise Exception("The campaigns column is not specified correctly. \n"
                        "Make sure it's a name, not an index!")


def _generate_campaigns(campaigns, client_id, id_column_name, campaign_column_name,
                        dataset, reserved_urls):
    campaign_url = "https://api.flowcode.com/v2/flowcode/batch/bulk-campaign"

    print("Creating Campaigns!")
    for campaign in campaigns:
        data = {
            "name": f"{campaign}",
            "display_name": f"{campaign} display",
            "client_id": client_id,
            "reserved_urls_unique": reserved_urls
        }
        try:
            response = requests.post(campaign_url, data)
            response.raise_for_status()
            print(response.text)
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 409:
                print(f"Campaign {campaign} already exists! Skipping creation")
            else:
                raise err
    return campaigns


def _preprocess_urls(campaigns, id_column_name, campaign_column_name,
                     redirect_url, dataset, pass_id_as_argument):
    campaign_data = []
    for campaign in campaigns:
        # get all rows where the campaign id is equal to our active campaign
        campaign_data.append(dataset.loc[dataset[campaign_column_name] == campaign])

    flowcodes = []  # initialise an empty list to store our url data

    for campaign in campaign_data:
        campaign_list = []
        for index, row in campaign.iterrows():
            if pass_id_as_argument:
                id_string = f"{id_column_name}"
                url_string = "".join([redirect_url, f"/id={row[id_string]}"])
            else:
                url_string = redirect_url
            campaign_list.append(
                              {
                                  "id": f"{row[id_string]}",
                                  "url_type": "URL",
                                  "url": url_string,
                              })
        flowcodes.append(campaign_list)
    return flowcodes


def _generate_urls(flowcodes, campaigns, client_id, smart_rules):
    codes_url = "https://api.flowcode.com/v2/flowcode/batch/bulk"
    responses = {}  # create a dictionary for storing campaigns and responses

    # send a request for each campaign in "flowcodes"
    for index, value in enumerate(flowcodes):
        if not value:  # deals with errors caused by empty campaigns
            pass
        else:
            if smart_rules:
                codes_data = {
                        "client_id":  client_id,
                        "campaign_name": f"{campaigns[index]}",
                        "urls": flowcodes[index],
                        "smart_rules": smart_rules
                }
            else:
                codes_data = {
                        "client_id":  client_id,
                        "campaign_name": f"{campaigns[index]}",
                        "urls": flowcodes[index],
                }

            # send the data as a json to support the formatting of "urls"
            try:
                flowcode_response = requests.post(codes_url, json=codes_data)
                flowcode_response.raise_for_status()
                responses[f'{campaigns[index]}'] = flowcode_response
            except requests.exceptions.HTTPError as err:
                if err.response.status_code == 409:
                    print(f"Some URLS in Campaign {campaigns[index]} already exist!"
                          "Skipping creation")
                else:
                    raise err
    return responses


def _process_url_responses(responses):
    print(responses)
    readable_responses = {campaign: response.json() for campaign, response in responses.items()}

    # creates a dicionary called generated_urls, with campaigns as keys and
    # a list of ids and qr code urls as values.
    generated_urls = {}
    for campaign, urls in readable_responses.items():
        generated_urls[f'{campaign}'] = []
        for url in urls:
            generated_urls[f'{campaign}'].append({"id": url['id'], "qr_code": url['images'][0]['url']})
    return generated_urls


def _generate_svgs(parent_dir, generated_urls):
    # create a new directory to store images
    root_dir = parent_dir + '/flowcode_images'
    if not os.path.exists(root_dir):
        os.mkdir(root_dir)

    for campaign, urls in generated_urls.items():
        campaign_dir = root_dir + f'/{campaign}'
        os.mkdir(campaign_dir)

        for url_object in urls:
            id = url_object['id']
            r = requests.get(url_object['qr_code'], allow_redirects=True)
            file_url = "".join([campaign_dir, f"/{id}.svg"])
            with open(file_url, 'wb') as handler:
                handler.write(r.content)

## TESTING THE FUNCTION


ad_data = pd.read_csv("/Users/finnmacken/Desktop/Flowcode/api-ads/ads.csv")
ad_data = ad_data.sample(n=5, random_state=5)

# ad_data

# ad_data['xyz_campaign_id'].unique()


# ad_data.loc[ad_data['xyz_campaign_id'] == 936]

# _preprocess_urls(campaigns, id_column_name, campaign_column_name, redirect_url, dataset, pass_id_as_argument)

generate_flowcodes(client_id="d929d46a-7eba-11ec-90d6-0242ac120003",
                   id_column_name='ad_id',
                   campaign_column_name="xyz_campaign_id",
                   redirect_url="http://www.flowcode.com",
                   dataset=ad_data,
                   parent_dir="/Users/finnmacken/Desktop/Flowcode/api-ads")
