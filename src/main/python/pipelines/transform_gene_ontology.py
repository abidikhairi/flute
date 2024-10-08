import argparse
import obonet
import pandas as pd
from tqdm import tqdm


def main(args):
    input_file = args.input_file
    output_file = args.output_file
    
    print(f"Reading Gene Onotlogy file: {input_file}")
    graph = obonet.read_obo(input_file)
    num_nodes = graph.number_of_nodes()
    
    print(f"Graph loaded successfully: {num_nodes} nodes, {graph.number_of_edges()} edges")
    
    data = {
        "id": [],
        "name": [],
        "namespace": [],
    }
    
    for node, obj in tqdm(graph.nodes(data=True), total=num_nodes):
        data['id'].append(node)
        data['name'].append(obj['name'])
        data['namespace'].append(obj['namespace'])
    
    terms_df = pd.DataFrame(data=data)
    
    print(f"Saving dataframe to: {output_file}")
    terms_df.to_csv(output_file, sep='\t', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--input-file', type=str, required=True, help='Path to gene_onotlogy.obo')
    parser.add_argument('--output-file', type=str, required=True, help='Path to gene_onotlogy.csv')
    
    args = parser.parse_args()
    main(args)
