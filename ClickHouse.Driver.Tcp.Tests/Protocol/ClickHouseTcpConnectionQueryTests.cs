using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

// Drives QueryAsync over a scripted server stream: the response shapes and failure modes a live server
// can't be made to produce on demand. A real SELECT round-trip is covered by the integration suite.
[TestFixture]
public class ClickHouseTcpConnectionQueryTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static readonly ClientHandshakeParameters Handshake = new()
    {
        ClientName = "test-client",
        Username = "default",
    };

    [Test]
    public async Task QueryAsync_DrainsResponse_YieldsRowBearingBlocksAndReturnsToReady()
    {
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await DataPacketAsync(Array.Empty<ulong>()),   // schema header (0 rows) — not yielded
            await DataPacketAsync(new ulong[] { 1, 2, 3 }),
            await ProgressPacketAsync(),
            await ProfileEventsPacketAsync(),               // always-present block, consumed and discarded
            EndOfStreamPacket());
        using var connection = await ConnectedAsync(script);

        var rows = await MaterializeAsync(connection);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Has.Count.EqualTo(1));
            CollectionAssert.AreEqual(new ulong[] { 1, 2, 3 }, rows[0]);
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task QueryAsync_MultipleRowBearingBlocks_YieldsAllInOrder()
    {
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await DataPacketAsync(new ulong[] { 1, 2 }),
            await DataPacketAsync(new ulong[] { 3, 4 }),
            EndOfStreamPacket());
        using var connection = await ConnectedAsync(script);

        var rows = await MaterializeAsync(connection);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Has.Count.EqualTo(2));
            CollectionAssert.AreEqual(new ulong[] { 1, 2 }, rows[0]);
            CollectionAssert.AreEqual(new ulong[] { 3, 4 }, rows[1]);
        });
    }

    [Test]
    public async Task QueryAsync_ServerExceptionMidStream_ThrowsButStaysReusable()
    {
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await DataPacketAsync(new ulong[] { 7 }),
            await ExceptionPacketAsync(241, "memory limit exceeded"));
        using var connection = await ConnectedAsync(script);

        var rows = new List<ulong[]>();
        var thrown = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            await foreach (Block block in connection.QueryAsync("SELECT 1", cancellationToken: None))
            {
                rows.Add(((IColumn<ulong>)block[0]).Values.ToArray());
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Code, Is.EqualTo(241));
            Assert.That(thrown.Message, Is.EqualTo("memory limit exceeded"));
            Assert.That(rows, Has.Count.EqualTo(1)); // the block before the exception was still surfaced
            CollectionAssert.AreEqual(new ulong[] { 7 }, rows[0]);
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task QueryAsync_UnexpectedPacket_ThrowsProtocolAndTerminates()
    {
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            PacketBytes(ServerPacketType.Pong)); // never valid in a query response
        using var connection = await ConnectedAsync(script);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await DrainAsync(connection));
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task QueryAsync_TransportEndsMidResponse_Terminates()
    {
        // A Data packet type with no block body: the read for the block name hits end of stream.
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await BytesAsync(w => w.WriteVarUInt((ulong)ServerPacketType.Data)));
        using var connection = await ConnectedAsync(script);

        Assert.ThrowsAsync<EndOfStreamException>(async () => await DrainAsync(connection));
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task QueryAsync_EnumerationAbandonedBeforeEndOfStream_Terminates()
    {
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await DataPacketAsync(new ulong[] { 1 }),
            await DataPacketAsync(new ulong[] { 2 }),
            EndOfStreamPacket());
        using var connection = await ConnectedAsync(script);

        await foreach (Block block in connection.QueryAsync("SELECT 1", cancellationToken: None))
        {
            _ = block;
            break; // stop after the first block, leaving the response partway read
        }

        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task QueryAsync_BeforeHandshake_ThrowsInvalidOperation()
    {
        using var connection = new ClickHouseTcpConnection(new ScriptedDuplexStream(Array.Empty<byte>()), socket: null);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await DrainAsync(connection));
    }

    [Test]
    public async Task QueryAsync_AfterTerminate_ThrowsObjectDisposed()
    {
        using var connection = await ConnectedAsync(await ServerHelloBytesAsync(54476));
        connection.Terminate();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await DrainAsync(connection));
    }

    private static async Task<ClickHouseTcpConnection> ConnectedAsync(byte[] script)
    {
        var connection = new ClickHouseTcpConnection(new ScriptedDuplexStream(script), socket: null);
        await connection.HandshakeAsync(Handshake, None);
        return connection;
    }

    // Materializes each yielded block's first UInt64 column into an owned array during iteration, since a block
    // is only valid for its iteration (its buffers are released when the enumerator advances).
    private static async Task<List<ulong[]>> MaterializeAsync(ClickHouseTcpConnection connection)
    {
        var rows = new List<ulong[]>();
        await foreach (Block block in connection.QueryAsync("SELECT 1", cancellationToken: None))
        {
            rows.Add(((IColumn<ulong>)block[0]).Values.ToArray());
        }

        return rows;
    }

    // Enumerates the response without reading block contents (for tests that assert an exception or state).
    private static async Task DrainAsync(ClickHouseTcpConnection connection)
    {
        await foreach (Block block in connection.QueryAsync("SELECT 1", cancellationToken: None))
        {
            _ = block;
        }
    }

    private static Task<byte[]> DataPacketAsync(ulong[] values)
        => BytesAsync(w =>
        {
            w.WriteVarUInt((ulong)ServerPacketType.Data);
            WriteUInt64Block(w, values);
        });

    private static Task<byte[]> ProfileEventsPacketAsync()
        => BytesAsync(w =>
        {
            w.WriteVarUInt((ulong)ServerPacketType.ProfileEvents);
            WriteUInt64Block(w, new ulong[] { 42 });
        });

    private static Task<byte[]> ProgressPacketAsync()
        => BytesAsync(w =>
        {
            w.WriteVarUInt((ulong)ServerPacketType.Progress);
            w.WriteVarUInt(1); // rows
            w.WriteVarUInt(2); // bytes
            w.WriteVarUInt(3); // total_rows
            w.WriteVarUInt(0); // wrote_rows
            w.WriteVarUInt(0); // wrote_bytes
            w.WriteVarUInt(0); // elapsed_ns
        });

    private static Task<byte[]> ExceptionPacketAsync(int code, string message)
        => BytesAsync(w =>
        {
            w.WriteVarUInt((ulong)ServerPacketType.Exception);
            w.WriteInt32(code);
            w.WriteString("DB::Exception");
            w.WriteString(message);
            w.WriteString("stack trace");
            w.WriteBool(false); // has_nested
        });

    private static byte[] EndOfStreamPacket()
        => BytesAsync(w => w.WriteVarUInt((ulong)ServerPacketType.EndOfStream)).GetAwaiter().GetResult();

    private static byte[] PacketBytes(ServerPacketType type)
        => BytesAsync(w => w.WriteVarUInt((ulong)type)).GetAwaiter().GetResult();

    // A block with a single UInt64 column named "x": name, block info, counts, column header, then the values.
    private static void WriteUInt64Block(ClickHouseBinaryWriter writer, ulong[] values)
    {
        writer.WriteString(string.Empty);
        BlockWriter.WriteBlockInfo(writer, BlockInfo.Default);
        writer.WriteVarUInt(1);                    // num_columns
        writer.WriteVarUInt((ulong)values.Length); // num_rows
        writer.WriteString("x");
        writer.WriteString("UInt64");
        writer.WriteBool(false);                   // has_custom_serialization
        foreach (ulong value in values)
        {
            writer.WriteUInt64(value);
        }
    }

    private static Task<byte[]> ServerHelloBytesAsync(int revision) => BytesAsync(w =>
    {
        w.WriteVarUInt((ulong)ServerPacketType.Hello);
        w.WriteString("ClickHouse");
        w.WriteVarUInt(25);
        w.WriteVarUInt(8);
        w.WriteVarUInt((ulong)revision);
        w.WriteString("UTC");
        w.WriteString("clickhouse-server");
        w.WriteVarUInt(0);
    });

    private static async Task<byte[]> BytesAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    private static byte[] Concat(params byte[][] arrays)
    {
        int length = 0;
        foreach (byte[] array in arrays)
        {
            length += array.Length;
        }

        byte[] result = new byte[length];
        int offset = 0;
        foreach (byte[] array in arrays)
        {
            Buffer.BlockCopy(array, 0, result, offset, array.Length);
            offset += array.Length;
        }

        return result;
    }
}
