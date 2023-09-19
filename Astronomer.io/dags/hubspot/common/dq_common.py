import pandas as pd
import warnings
import re
import urllib

# Global Settings.
warnings.simplefilter(action='ignore', category=FutureWarning)


class dq_common():
    def __init__(self):
        pass

    def dq_duplicate_check(self, df_in):
        '''
        This function is for dropping duplicates from a dataframe
        :param df_in: Souce dataframe in
        :return: Output dataframe less duplicates
        '''
        df_in.reset_index(drop=True, inplace=True)
        try:
            df_in.drop_duplicates(inplace=True)
        except:
            pass
        df_in.reset_index(drop=True, inplace=True)
        return df_in

    def dq_support_col_data_special_remove(self, x):
        '''
        This is an apply support function for removal of special characters
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        if 'http' in str(x):
            return x
        else:
            try:
                x = str(x).strip('$').replace('[]', '').replace("['", '').replace("']", '').replace("[", '').replace(
                    "]", '')
                x = x.replace("', '", ',').strip(' ').replace("','", ',').strip('\r').strip('\n')
                return x
            except:
                return x

    def dq_special_character_test(self, df_in, cols_to_skip=[]):
        '''
        This function will be used to support the removal of special characters.  If there are specific columns to
        skip this check on they are loaded in the cols_to_skip list.
        :param df_in: source dataframe
        :param cols_to_skip: columns that are to be skipped.
        :return: dataframe with appropriate transformation applied.
        '''
        df_bad = pd.DataFrame()
        df_in['change_counter'] = 0
        for col in df_in.columns.values:
            # if col == 'address_verification_endpoint':
            if col in cols_to_skip:
                pass
            elif df_in[col].dtype == 'datetime64[ns]':
                pass
            else:
                try:
                    df_in[col] = df_in[col].apply(self.dq_support_col_data_special_remove)
                except:
                    pass
        return df_in, df_bad

    def dq_support_strip_leading_quotes(self, x):
        '''
        This is an apply support function for removal of double quotes
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        return str(x).replace('"', '')

    def dq_replace_leading_double_quote(self, df_in, target_col=[]):
        '''
        This function is for the removal of " from from the data in the dataframe by column
        :param df_in: source data frame
        :param target_col: if a targeted column is required it would be passed in here.
        :return: Dataframe with all the double quotes removed.
        '''
        if len(target_col) == 0:
            for col in df_in.columns.values:
                try:
                    df_in[col] = df_in[col].apply(self.dq_support_strip_leading_quotes)
                except:
                    pass
            return df_in
        else:
            for col in target_col:
                if col in df_in.columns.values.tolist():
                    df_in[col] = df_in[col].apply(self.dq_support_strip_leading_quotes)
                else:
                    print('Column not found in data...Columns was:', col)
            return df_in

    def dq_support_none_from_nan(self, x):
        '''
        This is an apply support function for converting all variations of None to NONE
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        if str(x).upper() == 'NONE':
            return None
        elif str(x).upper() == 'NAN':
            return None
        elif str(x).upper() == 'NA':
            return None
        elif str(x).upper() == '':
            return None
        elif str(x).upper() == ' ':
            return None
        else:
            return x

    def dq_none_to_nan(self, df_in, target_col=[]):
        '''
        This function is to find any variation of none and replace with a consistent NONE.
        This also does apply to Canada's providences.
        :param df_in: Source data frame
        :param target_col: Any specific columns that may require this transformation.
        :return: Output dataframe with the applied functions.
        '''
        if len(target_col) == 0:
            for col in df_in.columns.values:
                try:
                    df_in[col] = df_in[col].apply(self.dq_support_none_from_nan)
                except:
                    pass
            return df_in
        else:
            for col in target_col:
                if col in df_in.columns.values.tolist():
                    df_in[col] = df_in[col].apply(self.dq_support_none_from_nan)
                else:
                    print('Column not found in data...Columns was:', col)
            return df_in

    def dq_support_strip_newline(self, x):
        '''
        This is an apply support function for removing all newlines from a string
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        return str(x).replace(r'\n', ' ')

    def dq_strip_newline(self, df_in, target_col=[]):
        '''
        This function is to find any occurrences of a newline and replace with a space.
        This also does apply to Canada's providences.
        :param df_in: Source data frame
        :param target_col: Any specific columns that may require this transformation.
        :return: Output dataframe with the applied functions.
        '''
        if len(target_col) == 0:
            for col in df_in.columns.values:
                try:
                    df_in[col] = df_in[col].apply(self.dq_support_strip_newline)
                except:
                    pass
            return df_in
        else:
            for col in target_col:
                if col in df_in.columns.values.tolist():
                    df_in[col] = df_in[col].apply(self.dq_support_strip_newline)
                else:
                    print('Column not found in data...Columns was:', col)
            return df_in

    def dq_support_comma_space_strip(self, x):
        '''
        This is an apply support function for replace and occurrences of ", " with only a comma
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        return str(x).replace(', ', ',')

    def dq_comma_space_strip(self, df_in, target_col=[]):
        '''
        This function is to find any occurrences of a comma space and replace with just a comma
        This also does apply to Canada's providences.
        :param df_in: Source data frame
        :param target_col: Any specific columns that may require this transformation.
        :return: Output dataframe with the applied functions.
        '''
        if len(target_col) == 0:
            for col in df_in.columns.values:
                try:
                    df_in[col] = df_in[col].apply(self.dq_support_comma_space_strip)
                except:
                    pass
            return df_in
        else:
            for col in target_col:
                if col in df_in.columns.values.tolist():
                    df_in[col] = df_in[col].apply(self.dq_support_comma_space_strip)
                else:
                    print('Column not found in data...Columns was:', col)
            return df_in

    def dq_support_strip_single_quotes(self, x):
        '''
        This is an apply support function for removing all single quotes from a string
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        return str(x).replace("'", '')

    def dq_strip_single_quotes(self, df_in, target_col=[]):
        '''
        This function is the removal of any occurrences of single quotes.
        This also does apply to Canada's providences.
        :param df_in: Source data frame
        :param target_col: Any specific columns that may require this transformation.
        :return: Output dataframe with the applied functions.
        '''
        if len(target_col) == 0:
            for col in df_in.columns.values:
                try:
                    df_in[col] = df_in[col].apply(self.dq_support_strip_single_quotes)
                except:
                    pass
            return df_in
        else:
            for col in target_col:
                if col in df_in.columns.values.tolist():
                    df_in[col] = df_in[col].apply(self.dq_support_strip_single_quotes)
                else:
                    print('Column not found in data...Columns was:', col)
            return df_in

    def dq_support_unicode_strip(self, x):
        '''
        This is an apply support function removing all unicode values from a string.  There is no replacement at this
        time strict removal only.
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        if '\\' in x:
            x_list = list(x)
            unicode_list = []
            counter = 0
            trap_counter = -1
            for c in x:
                if c == '\\':
                    build_string = ''
                    for i in range(counter, counter + 4):
                        build_string = build_string + x_list[i]
                    unicode_list.append(build_string)
                    trap_counter = counter + 3
                else:
                    if counter <= trap_counter:
                        counter += 1
                        continue
                    else:
                        unicode_list.append(x_list[counter])
                counter += 1
            new_string = ''
            for i in unicode_list:
                if '\\' in i:
                    continue
                else:
                    new_string = new_string + i
            return new_string
        else:
            return x

    def dq_strip_unicode(self, df_in, target_col=[]):
        '''
        This function is the removal of any unicode values from the string values.
        This also does apply to Canada's providences.
        :param df_in: Source data frame
        :param target_col: Any specific columns that may require this transformation.
        :return: Output dataframe with the applied functions.
        '''
        if len(target_col) == 0:
            for col in df_in.columns.values:
                try:
                    df_in[col] = df_in[col].apply(self.dq_support_unicode_strip)
                except:
                    pass
            return df_in
        else:
            for col in target_col:
                if col in df_in.columns.values.tolist():
                    df_in[col] = df_in[col].apply(self.dq_support_unicode_strip)
                else:
                    print('Column not found in data...Columns was:', col)
            return df_in

    def dq_support_dash_to_none(self, x):
        '''
        This is an apply support function for replacing a single dash to a none
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        if str(x) == '-':
            return None
        else:
            return x

    def dq_support_encode(self, x):
        '''
        This is an apply support function for URL encoding a string
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''

        return urllib.quote(str(x))

    def dq_support_remove_u(self, x):
        '''
        This is an apply support function for removing leading u'
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        return str(x).replace("u'", "'")

    def dq_support_remove_brackets(self, x):
        '''
        This is an apply support function for removing [''] and beginning and ending single quotes
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''

        return str(x).rstrip('\']').rstrip('\'').lstrip('[\'').lstrip('\'')

    def dq_dash_to_none(self, df_in, target_col=[]):
        '''
        This function is the replacement of a single dash to a NONE.
        This also does apply to Canada's providences.
        :param df_in: Source data frame
        :param target_col: Any specific columns that may require this transformation.
        :return: Output dataframe with the applied functions.
        '''
        if len(target_col) == 0:
            for col in df_in.columns.values:
                try:
                    df_in[col] = df_in[col].apply(self.dq_support_dash_to_none)
                except:
                    pass
            return df_in
        else:
            for col in target_col:
                if col in df_in.columns.values.tolist():
                    df_in[col] = df_in[col].apply(self.dq_support_dash_to_none)
                else:
                    print('Column not found in data...Columns was:', col)
            return df_in

    def dq_support_cleanup_to_none(self, x):
        '''
        This is an apply support function for replacing a specific character set to a none
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''

        if str(x) == '/':
            return None
        elif str(x) == '(not set)':
            return None
        elif str(x) == '(entrance)':
            return None
        elif str(x) == '(none)':
            return None
        elif str(x) == '(direct)':
            return None
        elif str(x) == '(not provided)':
            return None
        elif str(x) == 'ZZ':
            return None
        else:
            return x

    def dq_cleanup_to_none(self, df_in, target_col=[]):
        '''
        This function is the replacement of a specific character set to None.
        This also does apply to Canada's providences.
        :param df_in: Source data frame
        :param target_col: Any specific columns that may require this transformation.
        :return: Output dataframe with the applied functions.
        '''
        if len(target_col) == 0:
            for col in df_in.columns.values:
                try:
                    df_in[col] = df_in[col].apply(self.dq_support_cleanup_to_none)
                except:
                    pass
            return df_in
        else:
            for col in target_col:
                if col in df_in.columns.values.tolist():
                    df_in[col] = df_in[col].apply(self.dq_support_cleanup_to_none)
                else:
                    print('Column not found in data...Columns was:', col)
            return df_in

    def dq_support_state_abbreviate_test(self, x):
        '''
        This is an apply support function for converting all spelled out states to the abbreviated values.
        :param x: This is the value from the apply function that the transformation must occur on
        :return:Return would be X with the appropriate transformations.
        '''
        if str(x) == 'nan':
            return x

        us_state_abbrev = {
            'Alabama': 'AL',
            'Alaska': 'AK',
            'Arizona': 'AZ',
            'Arkansas': 'AR',
            'California': 'CA',
            'Colorado': 'CO',
            'Connecticut': 'CT',
            'Delaware': 'DE',
            'Florida': 'FL',
            'Georgia': 'GA',
            'Hawaii': 'HI',
            'Idaho': 'ID',
            'Illinois': 'IL',
            'Indiana': 'IN',
            'Iowa': 'IA',
            'Kansas': 'KS',
            'Kentucky': 'KY',
            'Louisiana': 'LA',
            'Maine': 'ME',
            'Maryland': 'MD',
            'Massachusetts': 'MA',
            'Michigan': 'MI',
            'Minnesota': 'MN',
            'Mississippi': 'MS',
            'Missouri': 'MO',
            'Montana': 'MT',
            'Nebraska': 'NE',
            'Nevada': 'NV',
            'New Hampshire': 'NH',
            'New Jersey': 'NJ',
            'New Mexico': 'NM',
            'New York': 'NY',
            'North Carolina': 'NC',
            'North Dakota': 'ND',
            'Ohio': 'OH',
            'Oklahoma': 'OK',
            'Oregon': 'OR',
            'Pennsylvania': 'PA',
            'Rhode Island': 'RI',
            'South Carolina': 'SC',
            'South Dakota': 'SD',
            'Tennessee': 'TN',
            'Texas': 'TX',
            'Utah': 'UT',
            'Vermont': 'VT',
            'Virginia': 'VA',
            'Washington': 'WA',
            'West Virginia': 'WV',
            'Wisconsin': 'WI',
            'Wyoming': 'WY',
            'British Columbia': 'BC',
            'Ontario': 'ON',
        }
        mapping_df = pd.DataFrame.from_dict(us_state_abbrev, orient='index')
        mapping_df.reset_index(drop=False, inplace=True)
        mapping_df.columns = ['fullname', 'shorthand']
        if x in mapping_df['shorthand'].values.tolist(): return x
        try:
            if len(x) == 2: return x
        except:
            return x
        if x not in mapping_df['fullname'].values.tolist(): return x

        target_df = mapping_df[(mapping_df['fullname'] == x)]
        try:
            target_df.reset_index(drop=True, inplace=True)
            return target_df.loc[0, 'shorthand']
        except:
            return x

    def dq_abbreviate_test(self, df_in, target_col=[]):
        '''
        This function is the checking of the state columns and verifying they are all in 2 letter abbreviated format.
        This also does apply to Canada's providences.
        :param df_in: Source data frame
        :param target_col: Any specific columns that may require this transformation.
        :return: Output dataframe with the applied functions.
        '''
        if len(target_col) == 0:
            for col in df_in.columns.values:
                if 'state_province' in col:
                    try:
                        df_in[col] = df_in[col].apply(self.dq_support_state_abbreviate_test)
                    except:
                        pass
                else:
                    continue
            return df_in
        else:
            for col in target_col:
                if col in df_in.columns.values.tolist():
                    df_in[col] = df_in[col].apply(self.dq_support_state_abbreviate_test)
                else:
                    print('Column not found in data...Columns was:', col)
            return df_in

    def dq_column_group_cleanup(self, df_in):
        '''
        This function is a single instance of going through a dataframe and applying the appropriate functions.  This function
        reduces the number of loops of review for each dataframe.  The less the loops the faster the processing time.
        :param df_in: Souce dataframe in
        :param target_col: Any specific targeted columns.  This aspect is not used yet, but should be a dictionary in the future
        :return: Output dataframe with the applied functions.
        '''
        for col in df_in.columns.values:
            if df_in[col].dtype == 'datetime64[ns]':
                continue
            #       try:
            #           df_in[col] = df_in[col].apply(self.dq_support_strip_leading_quotes)
            #       except:
            #           pass
            try:
                df_in[col] = df_in[col].apply(self.dq_support_cleanup_to_none)
            except:
                pass
            try:
                df_in[col] = df_in[col].apply(self.dq_support_none_from_nan)
            except:
                pass
            #       try:
            #           df_in[col] = df_in[col].apply(self.dq_support_strip_newline)
            #       except:
            #           pass
            #       try:
            #           df_in[col] = df_in[col].apply(self.dq_support_comma_space_strip)
            #       except:
            #           pass
            #       try:
            #           df_in[col] = df_in[col].apply(self.dq_support_strip_single_quotes)
            #       except:
            #           pass
            #       try:
            #           df_in[col] = df_in[col].apply(self.dq_support_unicode_strip)
            #       except:
            #           pass
            #       try:
            #           json_data = df_in[col].to_json(orient='records', lines='\n')
            #       except:
            #           df_in[col] = df_in[col].apply(self.dq_support_encode)
            try:
                df_in[col] = df_in[col].apply(self.dq_support_dash_to_none)
            except:
                pass
            try:
                df_in[col] = df_in[col].apply(self.dq_support_remove_brackets)
            except:
                pass

            try:
                df_in[col] = df_in[col].apply(self.dq_support_remove_u)
            except:
                pass

            if col == 'state_province':
                try:
                    df_in[col] = df_in[col].apply(self.dq_support_state_abbreviate_test)
                except:
                    pass

        return df_in

    def dq_grouped_cleanup(self, df_in, special_char_cols_to_skip=[]):
        '''
        This is a grouped function that will call all supported function in the list below
        :param df_in: Source dataframe
        :param special_char_cols_to_skip: This is a list of columns that are to be skipped for special character check
        :return: Output dataframe with all transformations.
        '''

        ##Duplicate Check
        df_good = self.dq_duplicate_check(df_in)

        # Special Character Removal from Data
        df_good, df_bad = self.dq_special_character_test(df_good, special_char_cols_to_skip)
        df_bad['failure_method'] = 'special_character_test'
        ### Individual Tests Replaced with Grouped##
        # df_good=self.dq_replace_leading_double_quote(df_good)
        # df_good = self.dq_non_to_nan(df_good)
        # df_good = self.dq_strip_newline(df_good)
        # df_good = self.dq_comma_space_strip(df_good)
        # df_good = self.dq_strip_single_quotes(df_good)
        # df_good = self.dq_strip_unicode(df_good)
        # df_good = self.dq_dash_to_none(df_good)
        # df_good = self.dq_abbreviate_test(df_good)
        df_good = self.dq_column_group_cleanup(df_good)
        df_bad_combined = df_bad.copy(deep=True)
        df_bad_combined = pd.concat([df_bad_combined, df_bad])
        return df_good, df_bad_combined

