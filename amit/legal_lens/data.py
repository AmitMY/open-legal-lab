import pandas as pd
import tiktoken
from path import Path
from tqdm import tqdm

COLUMNS = {
    'docref': 'Case Number',
    'url': 'Judgment URL',
    'date': 'Judgment Date',
    'year': 'Judgment Year',
    'proc_type': 'Proceeding Type',
    'merged_cases': 'Merged Cases',
    'division': 'Court Division',
    'division_type': 'Division Type',
    'n_judges': 'Number of Judges',
    'language': 'Language',
    'length': 'Judgment Length',
    'area_general': 'General Area of Law',
    'area_intermediate': 'Intermediate Area of Law',
    'area_detailed': 'Detailed Area of Law',
    'topic': 'Topic',
    'issue': 'Issue',
    'source_date': 'Date of Appealed Decision',
    'source_canton': 'Origin of Appealed Decision',
    'proc_duration': 'Duration of Federal Supreme Court Proceedings',
    'app_class': 'Appellant Class',
    'app_represented': 'Appellant Represented by Lawyer',
    'resp_class': 'Respondent Class',
    'resp_represented': 'Respondent Represented by Lawyer',
    'outcome': 'Outcome',
    'outcome_binary': 'Binary Outcome',
    'cited_bger': 'Cited Unpublished Federal Supreme Court Judgments',
    'n_cited_bger': 'Number of Cited Unpublished Federal Supreme Court Judgments',
    'cited_bge': 'Cited Published Federal Supreme Court Judgments',
    'n_cited_bge': 'Number of Cited Published Federal Supreme Court Judgments',
    'leading_case': 'Publication as Leading Case',
    'text': 'Judgment Text',
    'doi_version': 'Dataset Version DOI'
}

if __name__ == "__main__":
    bger_path = Path(__file__).parent.parent / "data" / "bger-2023-5-text.parquet"
    cases = pd.read_parquet(bger_path).to_dict(orient="records")
    leading_cases = [case for case in cases if case["leading_case"]]

    encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")

    output_path = Path(__file__).parent.parent / "data" / "leading-cases"

    # All text and document files uploaded to a GPT or to a ChatGPT conversation are capped at 2M tokens per file
    output_file_index = 0
    current_output_file = None
    current_output_file_token_count = 0

    for case in tqdm(leading_cases):
        case_as_text = [f"{readable_name}: {case[key]}" for key, readable_name in COLUMNS.items()]
        file_text = "\n".join(case_as_text) + "\n\n"
        tokens_count = len(encoding.encode(file_text))
        print("tokens_count", tokens_count)
        if current_output_file is None or current_output_file_token_count + tokens_count > 2_000_000:
            if current_output_file is not None:
                current_output_file.close()
            output_file_index += 1
            new_output_file_path = Path(__file__).parent.parent / "data" / f"leading-cases-{output_file_index}.txt"
            current_output_file = open(new_output_file_path, "w", encoding="utf-8")
            current_output_file_token_count = 0

        current_output_file_token_count += tokens_count
        current_output_file.write(file_text)
