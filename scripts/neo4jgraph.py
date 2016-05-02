import sys, random, json
from py2neo import neo4j, Graph, rel, Node, Relationship

# distance<t>source</s>target

def create_transitive_graph():

    # users = []
    dict_usr = {}
    user_json = open("/home/rachita/Desktop/mr_final_project/yelp_academic_dataset_user.json")
    for line in user_json:
        if len(line) > 0:
            data_users = json.loads(line)
            dict_usr[data_users["user_id"]] = (data_users["name"], data_users["friends"])

           # print(users)
    print("users added")
    user_json.close()

    dict_sim = {}
    sim_data = open("/home/rachita/Desktop/mr_final_project/usersSimilarity")
    for line in sim_data:
        if len(line) > 0:
            source = line.strip('\n').split(" ")[0].split("\t")[0]
            user_id = line.strip('\n').split(" ")[1].split("\t")[0]
            similarity = line.strip('\n').split("\t")[1]
            if source not in dict_sim:
                dict_sim.update({source:{user_id:similarity}})
            if user_id not in dict_sim[source]:
                d = {user_id: similarity}
                dict_sim[source].update(d)
            else:
                continue;

    print(dict_sim)
    sim_data.close()

    transitive_matrix = open("/home/rachita/Desktop/transitive_matrix")
    for line in transitive_matrix:
        if len(line) > 0:
            words = line.rstrip('\n').split("\t")
            distance = words[0]
            nodes = words[1].split(" ")
            source = nodes[0]
            target = nodes[1]

            if source in dict_usr:
                sname = dict_usr[source][0]
            if target in dict_usr:
                tname = dict_usr[target][0]

            print(sname, tname)
            sim = 0
            if source in dict_sim:
                if target in dict_sim[source]:
                    sim = dict_sim[source][target]
                    print (sim)


            node1 = graph.find_one("User", "user_id", source)
            node2 = graph.find_one("User", "user_id", target)

            if node1 == None:
                graph.create(Node("User", user_id=source, name=sname))
                print(node1)
            if node2 == None:
                graph.create(Node("User", user_id=target, name=tname))

            node1 = graph.find_one("User", "user_id", source)
            node2 = graph.find_one("User", "user_id", target)
            graph.create(Relationship(node1, sim, node2, degree=distance, similarity=sim))



if __name__ == '__main__':
    graph = Graph("http://neo4j:Neo4j@localhost:7474/db/data/")
    graph.cypher.execute("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")  # deleting existing data
    create_transitive_graph()

