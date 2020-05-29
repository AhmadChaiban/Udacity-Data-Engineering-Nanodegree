import configparser

def get_cluster_details():
    """
        Gets the redshift cluster credentials from the config file

        :return: all config information for the redshift cluster
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    return KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, \
           DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, \
           DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME


def add_endpoint_cluster(endpoint):
    """
    :param endpoint: redshift cluster endpoint
    :return: None, writing to config file
    """
    config = configparser.RawConfigParser()
    config.read_file(open('dwh.cfg'))
    config.set('DWH', 'DWH_ENDPOINT', endpoint)
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)


def get_endpoint():
    """
    :return: Cluster endpoint from config file
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    endpoint = config.get('DWH', 'DWH_ENDPOINT')
    return endpoint

