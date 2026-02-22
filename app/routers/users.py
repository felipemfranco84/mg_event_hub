from fastapi import APIRouter

router = APIRouter(prefix="/users", tags=["users"])

@router.get("/")
def get_users():
    return {"message": "Rota de usuários de mg_event_hub ativa"}
