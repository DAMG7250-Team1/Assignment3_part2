import os
import jinja2
from dotenv import load_dotenv

def render_sql_template(template_path, output_path):
    # Load environment variables
    load_dotenv()
    
    # Extract bucket name from S3 path
    s3_path = os.getenv('S3_PATH', '')
    # Convert https URL to bucket name
    if s3_path.startswith('https://'):
        s3_bucket = s3_path.split('.s3.')[0].split('//')[1]
    else:
        s3_bucket = s3_path.split('/')[0] if s3_path else ''
    
    # Prepare template variables
    template_vars = {
        's3_bucket': s3_bucket,
        'aws_role_arn': os.getenv('AWS_ROLE_ARN', '')
    }
    
    # Read the template
    with open(template_path, 'r') as f:
        template_content = f.read()
    
    # Create Jinja2 template
    template = jinja2.Template(template_content)
    
    # Render the template
    rendered_sql = template.render(**template_vars)
    
    # Write the rendered SQL to output file
    with open(output_path, 'w') as f:
        f.write(rendered_sql)

if __name__ == "__main__":
    template_path = "scripts/02_snowflake_setup.sql"
    output_path = "scripts/02_snowflake_setup_rendered.sql"
    render_sql_template(template_path, output_path)
    print(f"Rendered SQL template saved to {output_path}") 