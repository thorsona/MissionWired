"""
Script for downloading CSV data on constituents and emails, processing, and saving to a file.
Compatible with Python 3.6+
"""

import pandas as pd

## Hard-coding but leaving up here for easy editing in case these links change
## Ideally should be read in from a params/config file
CONSTITUENT_INFO = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv'
CONSTITUENT_EMAILS = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv'
CONSTITUENT_STATUS = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv'


def read_in_data(file):
    """
    Read in CSV data from file and return as a dataframe.
    Assert that data exists.
    
    Args:
        file (str): location of readable CSV file
    
    Returns:
        pd.DataFrame
    """
    df = pd.read_csv(file)

    assert len(df) > 0, "Data read failed"
    
    return df


class People(object):
    """Class for creating person-level primary email data for output to a file"""
    
    def __init__(self, email_df):  
        print("creating primary email data")
        self.emails = email_df[email_df.is_primary==1][['email', 'cons_email_id', 'cons_id']].copy()
        self.isEmpty = False if len(self.emails) > 0 else True


    def merge_data(self, info_df, sub_df):
        """
        Merge in data from constituent info and constituent subscription status files.

        Args:
            info_df (pd.DataFrame): constituent information data
            sub_df (pd.DataFrame): suscription status data
        """
        print("merging in constituent data")
        self.emails = self.emails.merge(info_df[['cons_id', 'create_dt', 'modified_dt', 'source']], 
                                        how='left', 
                                        on='cons_id')
        self.emails = self.emails.merge(sub_df[sub_df['chapter_id']==1][['cons_email_id', 'isunsub']],
                                        how='left', 
                                        on='cons_email_id')
        

    def sort_rename_cols(self):
        """Sort columns in corrent order and rename."""
        print("sorting and renaming columns")
        self.emails = self.emails[['email', 'source', 'isunsub', 'create_dt', 'modified_dt']].copy()
        self.emails.columns = ['email', 'code', 'is unsub', 'created_dt', 'updated_dt']


    def update_dtypes(self):
        """Ensure columns are coded as instructed and data types align."""
        print("recoding un sub flag")
        ## Is unsub flag
        self.emails.loc[self.emails['is unsub']==1, 'is unsub'] = True
        self.emails.loc[self.emails['is unsub']==0, 'is unsub'] = False
        # If an email is not present in the sub table, it is assumed to still be subscribed 
        # where chapter_id is 1
        self.emails.loc[self.emails['is unsub'].isnull(), 'is unsub'] = False

        self.emails['is unsub'] = self.emails['is unsub'].astype(bool)

        ## Created and updated dates
        print("recoding dates")
        for col in ['created_dt', 'updated_dt']:
            self.emails[col] = pd.to_datetime(self.emails[col], errors='raise')

        ## Email and code
        print("recoding email and source code")
        for col in ['email', 'code']:
            self.emails[col] = self.emails[col].astype(str)


    def save_to_csv(self):
        """Save resulting data file to CSV in working directory"""
        print("saving to file 'people.csv'")
        self.emails.to_csv("people.csv", index=False)
        
        
    def get_aggregates(self):
        """Creates aggregates out of the person email-level data and saves to CSV."""
        if len(self.emails) != self.emails['email'].nunique():
            print("Warning: data contains duplicates. Aggregates may be off.")
        
        self.aggregates = self.emails.drop_duplicates(subset=["email", "created_dt"]).copy()
        
        print("creating aggregate acquisition data")
        # turn datetime to just date part
        self.aggregates['created_dt'] = self.aggregates['created_dt'].dt.date
        
        ## make aggregates
        self.aggregates = pd.DataFrame(self.aggregates.groupby("created_dt").size()).reset_index(drop=False)
        
        ## rename columns
        self.aggregates.columns = ["acquisition date", "acquisitions"]
        
        ## turn acquisition date back into a date 
        self.aggregates["acquisition date"] = pd.to_datetime(self.aggregates['acquisition date'])
        self.aggregates["acquisition date"] = self.aggregates["acquisition date"].dt.date
        
        ## save to CSV
        print("saving to file 'aggregates.csv'")
        self.aggregates.to_csv("aggregates.csv", index=False)


def create_files():
    print("reading constituent info data")
    info_data = read_in_data(CONSTITUENT_INFO)

    print("reading constituent email data")
    email_data = read_in_data(CONSTITUENT_EMAILS)

    print("reading constituent status data")
    sub_status_data = read_in_data(CONSTITUENT_STATUS)

    ## Part 1
    people = People(email_data)
    people.merge_data(info_data, sub_status_data)
    if not people.isEmpty:
        people.sort_rename_cols()
        people.update_dtypes()
        people.save_to_csv()
    else:
        print("No primary emails in dataframe")

    ## Part 2
    people.get_aggregates()

if __name__=="__main__":
    create_files()