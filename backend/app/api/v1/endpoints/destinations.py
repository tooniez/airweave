"""The API module that contains the endpoints for destinations."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud, schemas
from app.api import deps
from app.platform.configs._base import Fields
from app.platform.locator import resource_locator

router = APIRouter()


@router.get("/list", response_model=list[schemas.Destination])
async def list_destinations(
    db: AsyncSession = Depends(deps.get_db),
    user: schemas.User = Depends(deps.get_user),
) -> list[schemas.Destination]:
    """Get all available destinations.

    Args:
    -----
        db: The database session
        user: The current user

    Returns:
    --------
        List[schemas.Destination]: A list of destinations
    """
    destinations = await crud.destination.get_all(db)
    return destinations


@router.get("/detail/{short_name}", response_model=schemas.DestinationWithConfigFields)
async def read_destination(
    *,
    db: AsyncSession = Depends(deps.get_db),
    short_name: str,
    user: schemas.User = Depends(deps.get_user),
) -> schemas.Destination:
    """Get destination by short name.

    Args:
    -----
        db: The database session
        short_name: The short name of the destination
        user: The current user

    Returns:
    --------
        destination (schemas.Destination): The destination
    """
    destination = await crud.destination.get_by_short_name(db, short_name)
    if not destination:
        raise HTTPException(status_code=404, detail="Destination not found")
    if destination.auth_config_class:
        auth_config_class = resource_locator.get_auth_config(destination.auth_config_class)
        fields = Fields.from_config_class(auth_config_class)
        destination_with_config_fields = schemas.DestinationWithConfigFields.model_validate(
            destination, from_attributes=True
        )
        destination_with_config_fields.config_fields = fields
        return destination_with_config_fields
    return destination
