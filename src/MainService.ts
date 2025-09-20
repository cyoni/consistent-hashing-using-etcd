import { v4 as uuidv4 } from "uuid";
import { Etcd3 } from "etcd3";
import HashRing from "hashring";

const instanceId = `instance-${uuidv4()}`;
const client = new Etcd3({ hosts: "http://localhost:2379" });
const NODE_TTL = 10;

const tickers = [
    "AAPL", "MSFT", "GOOG", "AMZN", "TSLA",
    "META", "NVDA", "NFLX", "ADBE", "INTC",
    "CSCO", "ORCL", "IBM", "QCOM", "AMD",
    "PYPL", "SHOP", "SQ", "CRM", "UBER",
    "LYFT", "BABA", "JD", "PDD", "TCEHY",
    "SPOT", "TWTR", "SNAP", "ZM", "DOCU",
    "ROKU", "DIS", "NKE", "SBUX", "MCD",
    "PEP", "KO", "WMT", "COST", "TGT",
    "BA", "GE", "CAT", "MMM", "HON",
    "GS", "JPM", "BAC", "WFC", "MS"
  ]; // prettier-ignore

class MainService {
  private ring: HashRing = new HashRing();

  run() {
    this.registerNode();
    this.watchNodes();

    setInterval(() => {
      this.getPrices();
    }, 5000);
  }

  async registerNode() {
    const lease = client.lease(NODE_TTL);
    await lease.put(`/nodes/${instanceId}`).value("online");

    lease.on("lost", async () => {
      console.log("Lease lost, retrying registration...");
      await this.registerNode();
    });

    console.log(`Registered ${instanceId}`);
  }

  async watchNodes() {
    const watcher = await client.watch().prefix("/nodes/").create();

    watcher.on("put", (res) => {
      const id = res.key.toString().split("/").pop();

      console.log(`Node joined: ${id}`);

      if (!this.ring.has(id)) {
        console.log(`Node joined: ${id}`);
        this.ring.add(id);

        this.printRebalance();
      }
    });

    watcher.on("delete", (res) => {
      const id = res.key.toString().split("/").pop();

      console.log(`Node left: ${id}`);
      this.ring.remove(id);
      this.printRebalance();
    });

    // Initial load of existing nodes
    const existingNodes = await client.getAll().prefix("/nodes/").keys();
    existingNodes.forEach((key) => {
      const id = key.toString().split("/").pop();

      console.log(`-> Node joined: ${id}`);
      this.ring.add(id);
      this.printRebalance();
    });

    console.log("Initial ring setup complete");
  }

  printRebalance() {
    console.log("rebalance");
  }

  getPrices() {
    let count = 0;

    for (const ticker of tickers) {
      if (this.ring.get(ticker) === instanceId) {
        count++;
      }
    }
    console.log("count", count);
  }
}

export default MainService;
