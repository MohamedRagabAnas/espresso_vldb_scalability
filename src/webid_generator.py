class WebIDGenerator:
    def init_anode_list(self, number_of_webids):
        """
        Initialize a list of agent nodes with WebIDs
        
        Args:
            number_of_webids: Number of WebIDs to generate
            
        Returns:
            List of agent dictionaries with email and webid fields
        """
        anode_list = []

        # for each WebID
        for i in range(number_of_webids):
            # construct a webid in the form of a sequentially-numbered email address
            email = 'agent' + str(i) + '@example.org'
            # add the web ID to the agent node
            webid = 'http://example.org/agent' + str(i) + '/profile/card#me'
            
            # create an agent object as a dictionary
            agent = {
                "email": email,
                "webid": webid
            }

            anode_list.append(agent)
            
        # return the list of agent nodes
        return anode_list
    
    def init_sanode_list(self, percs):
        
        snode_list = []

        for i in range(len(percs)):
            email = 'sagent' + str(i) + '@example.org'
            webid = 'http://example.org/sagent' + str(i) + '/profile/card#me'
            power=str(percs[i])
            
            sagent = {
                "email": email,
                "webid": webid,
                "power": power
            }
            
            snode_list.append(sagent)
        
        return snode_list
    
    def get_webids(self, number_of_webids):
        """
        Get just the WebID strings from the agent list
        
        Args:
            number_of_webids: Number of WebIDs to generate
            
        Returns:
            List of WebID strings
        """
        agents = self.init_anode_list(number_of_webids)
        return [agent["webid"] for agent in agents]


        