import os
from dotenv import load_dotenv
from deploy_snowpark import create_session, deploy_udfs, deploy_procedures, deploy_transformations

def test_local_deployment():
    """Test the deployment process locally"""
    # Load environment variables
    load_dotenv()
    
    try:
        # Create session
        session = create_session()
        print("✓ Connected to Snowflake")
        
        # Test UDF deployment
        deploy_udfs(session)
        print("✓ UDFs deployed successfully")
        
        # Test procedure deployment
        deploy_procedures(session)
        print("✓ Procedures deployed successfully")
        
        # Test transformation deployment
        deploy_transformations(session)
        print("✓ Transformations deployed successfully")
        
        print("✓ All components deployed successfully")
        
    except Exception as e:
        print(f"❌ Deployment failed: {str(e)}")
        raise
    finally:
        if 'session' in locals():
            session.close()

if __name__ == "__main__":
    test_local_deployment() 