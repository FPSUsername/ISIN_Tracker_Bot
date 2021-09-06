import settings
import logging
import asyncio
import aiosqlite
import os
import sys


class Database():
    def __init__(self, project_dir):
        self.logger = logging.getLogger('client.db')
        self.project_dir = project_dir
        self.database_file = self.project_dir + '/client.db'

    async def _init(self):
        self.conn = await aiosqlite.connect(self.database_file)
        await self.conn.execute("PRAGMA foreign_keys = 1")
        self.logger.debug("Database connection active")

    async def _commit(self):
        await self.conn.commit()

    async def _close(self):
        await self.conn.close()
        self.logger.debug("Database connection closed")

    async def create_database(self):
        # Not sure how settings_id and list_id work
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS Clients (
                user_id BIGINT UNIQUE PRIMARY KEY,
                settings_id BIGINT,
                list_id BIGINT
            );
            """
        )

        # Only one settings object per user (one-to-many)
        await self.conn.execute("""CREATE TABLE IF NOT EXISTS Settings
            (
                user_id        INTEGER UNIQUE PRIMARY KEY,
                Isin           BOOLEAN NOT NULL CHECK (Isin      IN (0,1)) DEFAULT 1,
                Bid            BOOLEAN NOT NULL CHECK (Bid       IN (0,1)) DEFAULT 1,
                Ask            BOOLEAN NOT NULL CHECK (Ask       IN (0,1)) DEFAULT 1,
                Day            BOOLEAN NOT NULL CHECK (Day       IN (0,1)) DEFAULT 1,
                Lever          BOOLEAN NOT NULL CHECK (Lever     IN (0,1)) DEFAULT 1,
                StopLoss       BOOLEAN NOT NULL CHECK (StopLoss  IN (0,1)) DEFAULT 1,
                Reference      BOOLEAN NOT NULL CHECK (Reference IN (0,1)) DEFAULT 1,
                FOREIGN KEY(user_id) REFERENCES Clients(user_id) ON DELETE CASCADE
            );
        """)

        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS Markets (
            Title          TEXT NOT NULL,
            Isin           TEXT UNIQUE PRIMARY KEY,
            Bid            REAL,
            Ask            REAL,
            Day            TEXT,
            Lever          REAL,
            Stoploss       TEXT,
            Reference      TEXT,
            Reference_perc TEXT,
            Ended          BOOLEAN NOT NULL CHECK (Ended IN (0,1)) DEFAULT 0
            );
            """
        )

        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS client_markets (
                user_id   BIGINT,
                object_id BIGINT,
                mark_del  BOOLEAN NOT NULL CHECK (mark_del IN (0,1)) DEFAULT 0,
                FOREIGN KEY(user_id) REFERENCES Clients(user_id) ON DELETE CASCADE,
                FOREIGN KEY(object_id) REFERENCES Markets(Isin) ON DELETE CASCADE
            )
            """
        )

    async def new_user(self, user):
        user_id = user.user_id

        # Add user
        await self.conn.execute("""INSERT OR IGNORE INTO Clients (user_id) VALUES (%d)""" % (user_id))

        # Enable all settings by default
        await self.conn.execute(
            """
            INSERT OR IGNORE INTO SETTINGS (user_id)
            VALUES ({user_id})
            """
            .format(user_id=user_id)
        )

        self.logger.debug(
            "User with ID %d has been added to the database." % (user_id))

        await self._commit()

    async def delete_user(self, user):
        user_id = user.user_id

        await self.conn.execute("DELETE FROM Clients WHERE user_id=%s" % (user_id))

        self.logger.debug(
            "User with ID %d has been deleted from the database." % (user_id))

        await self._commit()

    # EG. add sprinter
    async def insert_to_database(self, user, table, payload):
        user_id = user.user_id
        self.logger.debug("Insert into database")
        self.logger.debug("User ID: %d" % user_id)
        self.logger.debug("Table: %s" % table)
        self.logger.debug("Payload: {}".format(payload))

        if table == "Markets":
            # Should be insert or update actually
            insert_market = (
                """
                INSERT INTO "{table}"(Title, Isin, Bid, Ask, Day, Lever, Stoploss, Reference, Reference_perc)
                SELECT "{Title}", "{Isin}", "{Bid}", "{Ask}", "{Day}", "{Lever}", "{Stoploss}", "{Reference}", "{Reference_perc}"
                WHERE NOT EXISTS (SELECT * FROM "{table}" WHERE Isin="{Isin}")
                """
                .format(
                    table=table,
                    Title=payload['Title'],
                    Isin=payload['Isin'],
                    Bid=payload['Bied'],
                    Ask=payload['Laat'],
                    Day=payload['% 1 dag'],
                    Lever=payload['Hefboom'],
                    Stoploss=payload['Stop loss-niveau'],
                    Reference=payload['Referentiekoers_1'],
                    Reference_perc=payload['Referentiekoers_2'])
            )

        if table == "client_markets":
            insert_client_market = (
                """
                INSERT INTO "{table}"(user_id, object_id)
                SELECT "{user_id}", "{object_id}"
                WHERE NOT EXISTS (SELECT * FROM "{table}" WHERE user_id="{user_id}" AND object_id="{object_id}")
                """
                .format(table=table, user_id=user_id, object_id=payload['Isin'])
            )

        if table == "SETTINGS":
            insert_settings = (
                """
                REPLACE INTO "{table}"(User_ID, Isin, Bid, Ask, Day, Lever, StopLoss, Reference)
                VALUES ("{user_id}", "{isin}", "{bid}", "{ask}", "{day}", "{lever}", "{stoploss}", "{reference}")
                """
                .format(
                    table=table,
                    user_id=user_id,
                    isin=payload['Isin'],
                    bid=payload['Bid'],
                    ask=payload['ask'],
                    day=payload['day'],
                    lever=payload['lever'],
                    stoploss=payload['stoploss'],
                    reference=payload['reference']
                )
            )

        try:
            # Executing the SQL command
            if table == "Markets":
                await self.conn.execute(insert_market)
            if table == "client_markets":
                await self.conn.execute(insert_client_market)
            if table == "Settings":
                await self.conn.execute(insert_settings)

            # Commit your changes in the database
            await self._commit()

        except Exception as e:
            self.logger.error(e)
            # Rolling back in case of error
            await self.conn.rollback()

    # EG. remove sprinter from user
    async def delete_from_database(self, user, table, payload):
        user_id = user.user_id
        self.logger.debug("Delete from database")
        self.logger.debug("User ID: %d" % user_id)
        self.logger.debug("Table: %s" % table)
        self.logger.debug("Payload: {}".format(payload))

        if table == "client_markets":
            delete = (
                """
                DELETE FROM "{table}" WHERE user_id="{user_id}" AND object_id="{isin}"
                """
                .format(table=table, user_id=user_id, isin=payload['Isin'])
            )

        try:
            # Executing the SQL command
            if table == "client_markets":
                await self.conn.execute(delete)

            # Commit your changes in the database
            await self._commit()

        except Exception as e:
            self.logger.error(e)
            # Rolling back in case of error
            await self.conn.rollback()

    # EG. update user settings
    # Not completed
    async def update_database(self, user, table, payload):
        # Payload is a dictionary with the data that you want to update to the column(s).
        # Eg. [[{sprinter1}, {sprinter2}], [{unavailable_sprinter}]]
        user_id = user.user_id
        self.logger.debug("Update database")
        self.logger.debug("User ID: %d" % user_id)
        self.logger.debug("Table: %s" % table)
        self.logger.debug("Payload: {}".format(payload))

        if table == "Markets":
            # only active ones are updated
            update_markets = []
            for item in payload[0]:
                update_markets.append(
                    """
                    UPDATE Markets SET Title="{Title}", Bid="{Bid}", Ask="{Ask}", Day="{Day}", Lever="{Lever}", Stoploss="{Stoploss}", Reference="{Reference}", Reference_perc="{Reference_perc}", Ended="{Ended}"  WHERE Isin="{Isin}"
                    """
                    .format(
                        Title=item['Title'],
                        Isin=item['Isin'],
                        Bid=item['Bied'],
                        Ask=item['Laat'],
                        Day=item['% 1 dag'],
                        Lever=item['Hefboom'],
                        Stoploss=item['Stop loss-niveau'],
                        Reference=item['Referentiekoers_1'],
                        Reference_perc=item['Referentiekoers_2'],
                        Ended=item['Ended'])
                )
            if len(payload) > 1:
                for item in payload[1]:
                    update_markets.append(
                        """
                        UPDATE Markets SET Ended="{Ended}"  WHERE Isin="{Isin}"
                        """
                        .format(Isin=item['Isin'], Ended=item['Ended'])
                    )

        if table == "Settings":
            settings = ""
            for key, value in payload.items():
                settings += '"{}"="{}" '.format(key, value)

            update_settings = (
                """
                UPDATE Settings SET {settings} WHERE user_id="{user_id}"
                """
                .format(
                    settings=settings,
                    user_id=user_id
                )
            )

        if table == "client_markets":

            # This is not good, but it should work for now
            for key, value in payload.items():
                isin = key
                setting = value

            update_client_markets = (
                """
                UPDATE client_markets SET mark_del={setting} WHERE user_id="{user_id}" AND object_id="{isin}"
                """
                .format(
                    setting=setting,
                    user_id=user_id,
                    isin=isin
                )
            )

        try:
            # Executing the SQL command
            if table == "Markets":
                for item in update_markets:
                    await self.conn.execute(item)
            if table == "Settings":
                await self.conn.execute(update_settings)
            if table == "client_markets":
                await self.conn.execute(update_client_markets)

            # Commit your changes in the database
            await self._commit()

        except Exception as e:
            self.logger.error(e)
            # Rolling back in case of error
            await self.conn.rollback()

    # EG. get list of sprinters
    async def read_database(self, user, table, payload=None):
        user_id = user.user_id
        self.logger.debug("User ID: %d" % user_id)
        self.logger.debug("Table: %s" % table)
        self.logger.debug("Payload: {}".format(payload))

        if table in ["client_markets"]:
            read_client = (
                """
                SELECT *
                FROM "{table}" AS c
                JOIN Markets as m on m.Isin = c.object_id
                WHERE c.user_id="{user_id}"
                ORDER BY m.Title ASC
                """
                .format(table=table, user_id=user_id)
            )

        if table == "Markets":
            read_markets = (
                """
                SELECT * FROM "{table}" WHERE Isin="{Isin}"
                """
                .format(table=table, Isin=payload)
            )

        if table == "Settings":
            read_settings = (
                """
                SELECT * FROM "{table}" WHERE user_id="{user_id}"
                """
                .format(table=table, user_id=user_id)
            )

        results = None
        try:
            # Executing the SQL command
            if table == "client_markets":
                data = await self.conn.execute(read_client)
                user_data = await data.fetchall()
                results = {}
                for item in user_data:
                    results[item[1]] = item[2]

            if table == "Markets":
                data = await self.conn.execute(read_markets)
                column_data = await data.fetchone()
                column_names = [col[0] for col in data.description]
                results = dict(zip(column_names, column_data))

            if table == "Settings":
                data = await self.conn.execute(read_settings)
                column_data = await data.fetchone()
                column_names = [col[0] for col in data.description]
                results = dict(zip(column_names, column_data))


        except Exception as e:
            self.logger.error(e)

        return results

    # For debugging purposes only
    async def print_database(self):
        self.logger.warning("Printing database")

        self.logger.warning("Clients:")
        cursor = await self.conn.execute("SELECT * from Clients")
        results = await cursor.fetchall()
        for row in results:
            self.logger.warning(row)

        self.logger.warning("Markets:")
        cursor = await self.conn.execute("SELECT * from Markets")
        results = await cursor.fetchall()
        for row in results:
            self.logger.warning(row)

        self.logger.warning("Client_Markets:")
        cursor = await self.conn.execute("SELECT * from client_markets")
        results = await cursor.fetchall()
        for row in results:
            self.logger.warning(row)

        self.logger.warning("Settings:")
        cursor = await self.conn.execute("SELECT * from Settings")
        results = await cursor.fetchall()
        for row in results:
            self.logger.warning(row)
