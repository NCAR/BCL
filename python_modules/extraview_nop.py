#!/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 
#Copyright (c) 2017, University Corporation for Atmospheric Research
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without 
#modification, are permitted provided that the following conditions are met:
#
#1. Redistributions of source code must retain the above copyright notice, 
#this list of conditions and the following disclaimer.
#
#2. Redistributions in binary form must reproduce the above copyright notice,
#this list of conditions and the following disclaimer in the documentation
#and/or other materials provided with the distribution.
#
#3. Neither the name of the copyright holder nor the names of its contributors
#may be used to endorse or promote products derived from this software without
#specific prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
#AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
#ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
#LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
#CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
#SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
#INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
#WHETHER IN CONTRACT, STRICT LIABILITY,
#OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

class client:
    """ Extraview Client """
    config	 = None

    def __init__(self, user, password, url):
        self.config = None
        #self.config = config
    
    def get_field_allowed(self, field, parentfield = None, parentvalue = None):
        """
        @brief Get list of allowed values for a given Extraview Field
        @note this will cache fields with out parents as they appear not to change
        @param field field name to query
        @param parentfield field name of parent field or NULL
        @param parentvalue value of parent field or NULL
        @return array of id => value or FALSE on error
        """
        assert(False)    
  
    
    def get_field_value_to_field_key(self, field, value, parentfield = None, parentvalue = None):
        """
        @brief Get Extraview Field index (case insensitive)
        EV has alot of fields that are enums. takes the given field and 
        looks up the allowed values and their keys. compares those values
        to extrat the correct key.
        @param field field name to query
        @param parentfield field name of parent field or NULL
        @param value value to compare against possible field values
        @param parentvalue value of parent field or NULL  
        @param EVSETUP extraview setup array
        @return ev key for given field for value or FALSE on error
        """
        return None
    
    def get_group_id(self, group):
        """ Get Extraview Group ID (case insensitive) """
        return None
    
    def get_group_members(self, group):
        """ Get Groups Members from an Extraview group """
        return None
  
    def get_group_member(self, group, user, allow_nonmember = False):
        """ 
         * @brief Check if user is member of group and get EV name for user
         * @param group Name of the group (note in EV all names are capped)
         * @param user name of user (can be NULL to unassign user)
         * @param EVSETUP extraview setup array
         * @param allow_nonmember allow a non-member of group to be returned (EV group membership is not enforced)
         * @return user EV name or FALSE on error
        """
        return None
  
    def create(self, originator, group, user, title, description, fields = {}):
        """
        @brief Create new Extraview Issue (aka ticket)
        @param group Name of the group (note in EV all names are capped) or NULL
        @param user name of user or NULL
        @param title Issue title
        @param description Issue description
        @param fields array of fields to set (can override defaults)
        @param error set if return is false with EV error
        @return ev response or FALSE on error
        @see http://docs.extraview.com/site/extraview-64/application-programming-interface/insert 
        """
        return None
  
    def update(self, id, fields):
        """
        @brief Update Extraview Issue
        @param id Extraview issue id
        @param fields array containing field_name => new value
         The field names can be retrieving using a get on an issue
        @return ev response or FALSE on error
        """
        return None
    
    def assign_group(self, id, group, user = None, fields = {}):
        """
        @brief Assign/transfer a Given user/group to a EV Issue
        @param id EV issue id
        @param group Name of the group (note in EV all names are capped)
        @param user name of user (can be NULL to unassign user)
        @return ev response or FALSE on error
        """
        return None

    def search(self, fields, max):
        """
        @brief Search for EV issues
        @param field array of fields to search with their values
        @param max max number of issues to return 
        """
        return None
  
    def get_priority(self, priority):
        """ Get EV priority field value from string priority"""
        return None
    
    def resolve_config_fields(self, params, config_params):
        """ Auto resolve out config Param fields that are marked with a * at start of name """
        return None

    def close(self, id, comment, fields = {}):
        """ Close extraview ticket """
        return None
    
    def add_resolver_comment(self, id, comment, fields = {}):
        """ add resolver (admin only) comment extraview ticket """
        return None

    def add_user_comment(self, id, comment, fields = {}):
        """ add comment to user in extraview ticket """
        return None

    def get_issue(self, id):
        """
        @brief Retrieve Extraview Issue
        @param id Extraview issue id
        @return XML array of issue or FALSE on error
        """
        return None
