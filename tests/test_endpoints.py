import pytest

@pytest.mark.asyncio
async def test_register(test_client):
    user_payload = {'username': 'emperor22', 
                    'first_name': 'andi', 
                    'last_name': 'putra', 
                    'email': 'algiffaryriony@gmail.com'}
    res = await test_client.post('/register_user', json=user_payload)
    
    print(res.text)
    
    assert res.status_code == 200