#!/usr/bin/env python

def remove_colors(text):
    """ Remove console color codes """

    #Create regex that will match all BASH color codes
    color_regex = re.compile("\033\[[0-9;]+m") 

    color_regex.sub('', text)

    return text

