import json
import os
from discord.ext import commands
import discord
import asyncio
from collections import defaultdict
from io import BytesIO
from pathlib import Path
import plotly.graph_objects as go
import datetime
import pymysql.cursors
import time
import traceback
import arrow
import json

# fucking mess


class Activity(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        self.config = json.load(open(Path("config/sql.json").resolve()))

        self.connect()
        self.connection.close()  # wow

    def connect(self):
        self.connection = pymysql.connect(host=self.config["host"],
                                          user=self.config["user"],
                                          password=self.config["password"],
                                          db=self.config["db"],
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor
                                          )
        self.connection.autocommit(True)

    def get_name(self, id: int, table: str):
        sql = f"SELECT * FROM `discord`.`{table}` WHERE `id` = %s"
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (id))
            _return = cursor.fetchone()["name"]
            cursor.close()
            return _return

    def get_username_from_id(self, id: int):
        return self.get_name(id, "users")

    def get_channel_name_from_id(self, id: int):
        return self.get_name(id, "channels")

    def get_server_name_from_id(self, id: int):
        return self.get_name(id, "servers")

    def get_channel_ids_from_server_id(self, id: int):
        channel_ids_in_server = []
        sql = "SELECT * FROM `discord`.`channels` WHERE `serverid` = %s"
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (id))
            for val in cursor.fetchall():
                channel_ids_in_server.append(val["id"])
            cursor.close()
        return channel_ids_in_server

    @commands.command()
    @commands.is_owner()
    async def update(self, ctx):
        if self.connection.open:
            return
        try:
            message_counts = defaultdict(int)
            usernames = defaultdict(str)
            self.connect()

            sql = "SELECT `timestamp` FROM `discord`.`channels` WHERE `id` = %s"
            timestamp = None
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (ctx.message.channel.id))
                try:
                    timestamp = cursor.fetchone()["timestamp"]
                except:
                    pass
                cursor.close()

            start_timestamp = datetime.datetime.utcnow() + datetime.timedelta(microseconds=10)
            async for msg in ctx.channel.history(limit=None, after=timestamp):
                message_counts[msg.author.id] += 1
                if msg.author.id not in usernames.values():
                    usernames[msg.author.id] = msg.author.name

            for key in [*message_counts]:
                if key == 0:
                    continue
                with self.connection.cursor() as cursor:
                    sql = "REPLACE INTO `discord`.`users` VALUES (%s, %s)"
                    cursor.execute(sql, (key, usernames[key]))

                    if timestamp is None:
                        sql = "REPLACE INTO `discord`.`activity` VALUES (%s, %s, %s)"
                        cursor.execute(
                            sql, (key, ctx.message.channel.id, message_counts[key]))
                    else:
                        sql = "UPDATE `discord`.`activity` SET `messages` = `messages` + %s WHERE `userid` = %s AND `channelid` = %s"
                        cursor.execute(
                            sql, (message_counts[key], key, ctx.message.channel.id))
                    cursor.close()

            messages_in_channel = 0
            sql = "SELECT * FROM `discord`.`activity` WHERE `channelid` = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (ctx.message.channel.id))
                for val in cursor.fetchall():
                    messages_in_channel += val["messages"]
                cursor.close()

            if timestamp is not None:
                sql = "UPDATE `discord`.`channels` SET `name` = %s, `timestamp` = %s, `messages` = %s WHERE `id` = %s"
                with self.connection.cursor() as cursor:
                    cursor.execute(sql, (ctx.message.channel.name,
                                        start_timestamp,
                                        messages_in_channel,
                                        ctx.message.channel.id))
                    cursor.close()
            else:
                sql = "INSERT INTO `discord`.`channels` (`id`, `serverid`, `name`, `timestamp`, `messages`) VALUES (%s, %s, %s, %s, %s)"
                with self.connection.cursor() as cursor:
                    cursor.execute(sql, (ctx.message.channel.id,
                                         ctx.message.guild.id if ctx.message.guild is not None else None,
                                         ctx.message.channel.name,
                                         start_timestamp,
                                         messages_in_channel))
                    cursor.close()

            if ctx.message.guild is not None:
                messages_in_server = 0
                for channel_id in self.get_channel_ids_from_server_id(ctx.message.guild.id):
                    sql = "SELECT * FROM `discord`.`activity` WHERE `channelid` = %s"
                    with self.connection.cursor() as cursor:
                        cursor.execute(sql, (channel_id))
                        for val in cursor.fetchall():
                            messages_in_server += val["messages"]
                        cursor.close()

                sql = "REPLACE INTO `discord`.`servers` VALUES (%s, %s, %s)"
                with self.connection.cursor() as cursor:
                    cursor.execute(sql, (ctx.message.guild.id,
                                         ctx.message.guild.name,
                                         messages_in_server))
                    cursor.close()
        except Exception:
            traceback.print_exc()
        finally:
            self.connection.close()
            print(f"done {ctx.message.id}")

    @commands.command()
    @commands.is_owner()
    async def update_per_hour(self, ctx):
        if self.connection.open:
            return
        try:
            data = defaultdict(int)
            self.connect()

            sql = "SELECT `hour_timestamp` FROM `discord`.`channels` WHERE `id` = %s"
            timestamp = None
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (ctx.message.channel.id))
                try:
                    res = cursor.fetchone()
                    timestamp = res["hour_timestamp"]
                except:
                    pass
                cursor.close()

            async for msg in ctx.channel.history(limit=None, after=timestamp):
                data[msg.created_at.replace(
                    minute=0, second=0, microsecond=0)] += 1

            with self.connection.cursor() as cursor:
                start_timestamp = datetime.datetime.utcnow() + datetime.timedelta(microseconds=10)
                if res is None:
                    sql = "INSERT INTO `discord`.`channels` (`id`, `serverid`, `name`, `hour_timestamp`, `timestamp`) VALUES (%s, %s, %s, %s, `timestamp`)"
                    cursor.execute(sql, (ctx.message.channel.id, ctx.message.guild.id if ctx.message.guild is not None else None,
                                         ctx.message.channel.name, start_timestamp))
                else:
                    sql = "UPDATE `discord`.`channels` SET `hour_timestamp` = %s WHERE `id` = %s"
                    cursor.execute(
                        sql, (start_timestamp, ctx.message.channel.id))
                cursor.close()

            for key in [*data]:
                if key == 0:
                    continue
                with self.connection.cursor() as cursor:
                    sql = "REPLACE INTO `discord`.`messages_per_hour` VALUES (%s, %s, %s)"
                    cursor.execute(
                        sql, (ctx.message.channel.id, key, data[key]))
                    cursor.close()
        except Exception:
            traceback.print_exc()
        finally:
            self.connection.close()
            print(f"done {ctx.message.id}")

    @commands.command()
    @commands.is_owner()
    async def activity_per_user(self, ctx):
        if self.connection.open:
            return
        try:
            self.connect()
            data = defaultdict(int)

            sql = "SELECT * FROM `discord`.`activity` WHERE `channelid` = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (ctx.message.channel.id))
                for val in cursor.fetchall():
                    data[self.get_username_from_id(
                        val["userid"])] = val["messages"]

            fig = go.Figure(
                data=[go.Pie(labels=list(data.keys()), values=list(data.values()))])
            img_bytes = fig.to_image(format="png", width=2240, height=1600)
            await ctx.send(file=discord.File(BytesIO(img_bytes), filename="guy.png"))
            fig.show()
        except Exception:
            traceback.print_exc()
        finally:
            self.connection.close()

    @commands.command()
    @commands.is_owner()
    async def activity_per_server(self, ctx):
        if self.connection.open:
            return
        try:
            self.connect()
            data = defaultdict(int)

            sql = "SELECT * FROM `discord`.`servers`"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, ())
                for val in cursor.fetchall():
                    data[val["name"]] = val["messages"]

            sql = "SELECT * FROM `discord`.`channels` WHERE `serverid` IS NULL"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, ())
                for val in cursor.fetchall():
                    data[val["name"]] = val["messages"]

            fig = go.Figure(
                data=[go.Bar(x=list(data.keys()), y=list(data.values()))])
            img_bytes = fig.to_image(format="png", width=2240, height=1600)
            await ctx.send(file=discord.File(BytesIO(img_bytes), filename="guy.png"))
            fig.show()
        except Exception:
            traceback.print_exc()
        finally:
            self.connection.close()

    @commands.command()
    @commands.is_owner()
    async def messages_per_user(self, ctx):
        if self.connection.open:
            return
        try:
            self.connect()
            users = []
            data = defaultdict(int)

            sql = "SELECT * FROM `discord`.`users`"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, ())
                for val in cursor.fetchall():
                    users.append(val["id"])
                cursor.close()

            for user in users:
                sql = "SELECT * FROM `discord`.`activity` WHERE `userid` = %s"
                with self.connection.cursor() as cursor:
                    cursor.execute(sql, (user))
                    for val in cursor.fetchall():
                        data[self.get_username_from_id(
                            user)] += val["messages"]
                    cursor.close()

            fig = go.Figure(
                data=[go.Pie(labels=list(data.keys()), values=list(data.values()))])
            img_bytes = fig.to_image(format="png", width=2240, height=1600)
            await ctx.send(file=discord.File(BytesIO(img_bytes), filename="guy.png"))
            fig.show()
        except Exception:
            traceback.print_exc()
        finally:
            self.connection.close()

    @commands.command()
    @commands.is_owner()
    async def user_messages_per_server(self, ctx, member: discord.User = None):
        if self.connection.open:
            return
        if member is None:
            member = ctx.author
        try:
            self.connect()
            data = defaultdict(int)

            sql = "SELECT * FROM `discord`.`activity` WHERE `userid` = %s AND `serverid` IS NULL"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (member.id))
                for val in cursor.fetchall():
                    data[self.get_channel_name_from_id(
                        val["channelid"])] += val["messages"]
                cursor.close()

            sql = "SELECT * FROM `discord`.`activity` WHERE `userid` = %s AND `serverid` IS NOT NULL"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (member.id))
                for val in cursor.fetchall():
                    data[self.get_server_name_from_id(
                        val["serverid"])] += val["messages"]
                cursor.close()

            fig = go.Figure(
                data=[go.Pie(labels=list(data.keys()), values=list(data.values()))])
            img_bytes = fig.to_image(format="png", width=2240, height=1600)
            await ctx.send(file=discord.File(BytesIO(img_bytes), filename="guy.png"))
            fig.show()
        except Exception:
            traceback.print_exc()
        finally:
            self.connection.close()

    @commands.command()
    @commands.is_owner()
    async def activity_per_hour(self, ctx):
        if self.connection.open:
            return
        try:
            self.connect()
            data = defaultdict(int)

            sql = "SELECT * FROM `discord`.`messages_per_hour` WHERE `channelid` = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (ctx.message.channel.id))
                for val in cursor.fetchall():
                    data[str(val["timestamp"])] = val["messages"]
                cursor.close()

            fig = go.Figure(
                data=[go.Scatter(x=list(data.keys()), y=list(data.values()))])
            img_bytes = fig.to_image(format="png", width=2240, height=1600)
            await ctx.send(file=discord.File(BytesIO(img_bytes), filename="guy.png"))
            fig.show()
        except Exception:
            traceback.print_exc()
        finally:
            self.connection.close()

    @commands.command()
    @commands.is_owner()
    async def activity_per_day(self, ctx):
        if self.connection.open:
            return
        try:
            self.connect()
            data = defaultdict(int)

            sql = "SELECT * FROM `discord`.`messages_per_hour` WHERE `channelid` = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (ctx.message.channel.id))
                for val in cursor.fetchall():
                    data[str(val["timestamp"].replace(hour=0))
                         ] += val["messages"]
                cursor.close()

            fig = go.Figure(
                data=[go.Scatter(x=list(data.keys()), y=list(data.values()))])
            img_bytes = fig.to_image(format="png", width=2240, height=1600)
            await ctx.send(file=discord.File(BytesIO(img_bytes), filename="guy.png"))
            fig.show()
        except Exception:
            traceback.print_exc()
        finally:
            self.connection.close()

    @commands.command()
    @commands.is_owner()
    async def activity_per_month(self, ctx):
        if self.connection.open:
            return
        try:
            self.connect()
            data = defaultdict(int)

            sql = "SELECT * FROM `discord`.`messages_per_hour` WHERE `channelid` = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (ctx.message.channel.id))
                for val in cursor.fetchall():
                    data[str(val["timestamp"].replace(
                        day=1, hour=0))] += val["messages"]
                cursor.close()

            fig = go.Figure(
                data=[go.Scatter(x=list(data.keys()), y=list(data.values()))])
            img_bytes = fig.to_image(format="png", width=2240, height=1600)
            await ctx.send(file=discord.File(BytesIO(img_bytes), filename="guy.png"))
            fig.show()
        except Exception:
            traceback.print_exc()
        finally:
            self.connection.close()


def setup(bot):
    bot.add_cog(Activity(bot))
