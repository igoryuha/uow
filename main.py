from typing import cast

from sqlalchemy import Connection, create_engine, Table, Column, Integer, String, MetaData, ForeignKey, select, CursorResult, bindparam


engine = create_engine("sqlite://")

metadata_obj = MetaData()

user_table = Table(
    "users",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String(64), nullable=False),
)

message_table = Table(
    "messages",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("body", String, nullable=False),
    Column("user_id", ForeignKey('users.id')),
)


metadata_obj.create_all(engine)

with engine.connect() as connection:
    connection.execute(
        user_table.insert().values([
            {'id': 1, 'name': 'bob'},
            {'id': 2, 'name': 'sam'},
            {'id': 3, 'name': 'von'},
        ])
    )
    connection.execute(
        message_table.insert().values([
            {'id': 1, 'body': 'body 1', 'user_id': 1},
            {'id': 2, 'body': 'body 2', 'user_id': 1},
            {'id': 3, 'body': 'body 3', 'user_id': 1},
            {'id': 4, 'body': 'body 4', 'user_id': 2},
        ])
    )
    connection.commit()


class Message:

    def __init__(self, message_id: int, body: str):
        self._message_id = message_id
        self._body = body

    @property
    def message_id(self) -> int:
        return self._message_id
    
    @property
    def body(self) -> str:
        return self._body

    def edit(self, body: str) -> None:
        self._body = body


class User:

    def __init__(self, user_id: int, name: str, messages: list[Message]):
        self._user_id = user_id
        self._name = name
        self._messages = messages

    @property
    def user_id(self) -> int:
        return self._user_id
    
    @property
    def name(self) -> str:
        return self._name

    def rename(self, new_name: str) -> None:
        self._name = new_name
    
    def edit_message(self, message_id: int, body: str) -> None:
        for message in self._messages:
            if message.message_id == message_id:
                message.edit(body)


class Registry:
    
    def __init__(self):
        self._mappers = {}

    def register_mapper(self, mapper_type, mapper):
        self._mappers[mapper_type] = mapper

    def get(self, mapper_type):
        mapper = self._mappers.get(mapper_type)
        if not mapper:
            raise Exception('Mapper not found')
        return mapper


class UnitOfWork:

    def __init__(self, registry: Registry, connection: Connection):
        self._new = {}
        self._dirty = {}

        self._registry = registry
        self._connection = connection

    def register_new(self, *, mapper, entity):
        self._new.setdefault(mapper, []).append(entity)

    def register_dirty(self, *, mapper, entity):
        self._dirty.setdefault(mapper, []).append(entity)

    def commit(self):
        for mapper_type, data in self._dirty.items():
            mapper = self._registry.get(mapper_type)
            mapper.update_all(data)

        self._connection.commit()


class UserMapper:

    def __init__(self, connection: Connection):
        self._connection = connection
    
    def update_all(self, users: list[User]):
        params = []

        for user in users:
            params.append({
                'name': user.name,
                'user_id': user.user_id,
            })

        stmt = (
            user_table.update()
            .values(name=bindparam('name'))
            .where(user_table.c.id==bindparam('user_id'))
        )

        self._connection.execute(stmt, params)
    
    def add(self, user: User) -> None:
        pass

    def add_all(self, users: list[User]) -> None:
        pass

    def delete(self, user: User) -> None:
        pass

    def with_id(self, user_id: int) -> User | None:
        pass

    def with_name(self, name: str) -> User | None:
        pass


class MessageMapper:

    def __init__(self, connection: Connection):
        self._connection = connection

    def update(self, message: Message):
        stmt = (
            message_table.update()
            .where(message_table.c.id == message.message_id)
            .values(
                id=message.message_id,
                body=message.body
            )
        )
        self._connection.execute(stmt)
    
    def update_all(self, messages: list[Message]) -> None:
        params = []

        for message in messages:
            params.append({
                'body': message.body,
                'message_id': message.message_id,
            })

        stmt = (
            message_table.update()
            .values(body=bindparam('body'))
            .where(message_table.c.id==bindparam('message_id'))
        )

        self._connection.execute(stmt, params)

    def add(self, message: Message) -> None:
        pass

    def add_all(self, messages: list[Message]) -> None:
        pass

    def delete(self, message: Message) -> None:
        pass

    def with_id(self, message_id: int) -> Message | None:
        pass


class MessageProxy:

    def __init__(self, message: Message, unit_of_work: UnitOfWork):
        self._message = message
        self._unit_of_work = unit_of_work

    @property
    def message_id(self) -> int:
        return self._message.message_id
    
    @property
    def body(self) -> str:
        return self._message.body

    def edit(self, body: str) -> None:
        self._unit_of_work.register_dirty(
            mapper=MessageMapper, entity=self._message,
        )
        return self._message.edit(
            body=body,
        )


class UserProxy:
    
    def __init__(self, user: User, unit_of_work: UnitOfWork):
        self._user = user
        self._unit_of_work = unit_of_work

    @property
    def user_id(self) -> int:
        return self._user.user_id
    
    @property
    def name(self) -> str:
        return self._user.name

    def rename(self, new_name: str) -> None:
        self._unit_of_work.register_dirty(
            mapper=UserMapper, entity=self._user,
        )
        return self._user.rename(
            new_name=new_name,
        )
    
    def edit_message(self, message_id: int, body: str) -> None:
        return self._user.edit_message(
            message_id=message_id,
            body=body,
        )


class UserRepository:

    def __init__(self, connection: Connection, unit_of_work: UnitOfWork):
        self._connection = connection
        self._unit_of_work = unit_of_work

    def _load(self, result: CursorResult) -> User:
        proxy_messages = []

        for row in result:
            message = Message(
                    message_id=row.message_id,
                    body=row.message_body,
                )
            message_proxy = MessageProxy(
                    message=message,
                    unit_of_work=self._unit_of_work,
                )
            proxy_messages.append(message_proxy)
        
        messages = cast(
                list[Message], proxy_messages,
            )

        user = User(
            user_id=row.user_id,
            name=row.user_name,
            messages=messages
        )

        user_proxy = UserProxy(
            user=user, 
            unit_of_work=unit_of_work,
        )

        return cast(
            User, user_proxy
        )

    def with_id(self, user_id: int) -> User:
        stmt = (
            select(
                user_table.c.id.label('user_id'),
                user_table.c.name.label('user_name'),
                message_table.c.id.label('message_id'),
                message_table.c.body.label('message_body'),
            )
            .join(message_table)
            .where(user_table.c.id == user_id)
        )
        result = self._connection.execute(
            stmt,
        )
        return self._load(result)
    

class Interactor:

    def __init__(
            self, 
            user_repository: UserRepository,
            unit_of_work: UnitOfWork,
    ):
        self._user_repository = user_repository
        self._unit_of_work = unit_of_work

    def execute(self):
        user = user_repository.with_id(1)

        user.edit_message(1, 'new message body 1')
        user.edit_message(2, 'new message body 2')

        user.rename('new username')

        unit_of_work.commit()


with engine.connect() as connection:
    registry = Registry()

    registry.register_mapper(MessageMapper, MessageMapper(connection))
    registry.register_mapper(UserMapper, UserMapper(connection))

    unit_of_work = UnitOfWork(
        registry=registry,
        connection=connection,
    )

    user_repository = UserRepository(
        connection=connection,
        unit_of_work=unit_of_work,
    )

    interactor = Interactor(
        user_repository=user_repository,
        unit_of_work=unit_of_work,
    )

    interactor.execute()

    message_1 = connection.execute(
        select(message_table).where(message_table.c.id == 1)
    ).one()
    message_2 = connection.execute(
        select(message_table).where(message_table.c.id == 2)
    ).one()

    print(message_1.body)
    print(message_2.body)

    user_1 = connection.execute(
        select(user_table).where(user_table.c.id == 1)
    ).one()

    print(user_1.name)
