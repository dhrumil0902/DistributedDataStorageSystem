package testing;

import app_kvServer.KVServer;
import ecs.ECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;
import app_kvECS.ECSClient;
import junit.framework.TestCase;
import org.junit.experimental.theories.Theories;
import shared.BST;
import shared.messages.CoordMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.utils.HashUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BSTTests extends TestCase {

    @Test
    public void testCreateReplicatedRange() {
        BST bst = new BST();
        bst.put("2", new ECSNode("", "localhost", 5000, new String[] {"1", "2"}));
        bst.put("3", new ECSNode("", "localhost", 5001, new String[] {"2", "3"}));
        bst.put("4", new ECSNode("", "localhost", 5002, new String[] {"3", "4"}));
        bst.put("5", new ECSNode("", "localhost", 5003, new String[] {"4", "5"}));
        bst.put("6", new ECSNode("", "localhost", 5004, new String[] {"5", "6"}));
        bst.put("7", new ECSNode("", "localhost", 5005, new String[] {"6", "7"}));
        bst.put("1", new ECSNode("", "localhost", 5006, new String[] {"7", "1"}));

        BST replicatedRangeBst = bst.createReplicatedRange();

        assertEquals(replicatedRangeBst.get("1").getNodeHashRange()[0], "5");
        assertEquals(replicatedRangeBst.get("1").getNodeHashRange()[1], "1");

        assertEquals(replicatedRangeBst.get("2").getNodeHashRange()[0], "6");
        assertEquals(replicatedRangeBst.get("2").getNodeHashRange()[1], "2");

        assertEquals(replicatedRangeBst.get("3").getNodeHashRange()[0], "7");
        assertEquals(replicatedRangeBst.get("3").getNodeHashRange()[1], "3");

        assertEquals(replicatedRangeBst.get("4").getNodeHashRange()[0], "1");
        assertEquals(replicatedRangeBst.get("4").getNodeHashRange()[1], "4");
    }

    @Test
    public void testCreateReplicatedRange_smallerRange() {
        BST bst = new BST();
        bst.put("2", new ECSNode("", "localhost", 5000, new String[] {"1", "2"}));
        bst.put("1", new ECSNode("", "localhost", 5001, new String[] {"2", "1"}));

        BST replicatedRangeBst = bst.createReplicatedRange();

        assertEquals(replicatedRangeBst.get("1").getNodeHashRange()[0], "1");
        assertEquals(replicatedRangeBst.get("1").getNodeHashRange()[1], "1");

        assertEquals(replicatedRangeBst.get("2").getNodeHashRange()[0], "2");
        assertEquals(replicatedRangeBst.get("2").getNodeHashRange()[1], "2");
    }

    @Test
    public void testCreateReplicatedRange_oneRange() {
        BST bst = new BST();
        bst.put("1", new ECSNode("", "localhost", 5000, new String[] {"1", "1"}));

        BST replicatedRangeBst = bst.createReplicatedRange();

        assertEquals(replicatedRangeBst.get("1").getNodeHashRange()[0], "1");
        assertEquals(replicatedRangeBst.get("1").getNodeHashRange()[1], "1");
    }
}