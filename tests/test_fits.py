"""Target admission: may this node receive this pod, and does it stay healthy after."""
import balancer as B
from conftest import GB, fill, make_node


POD_CPU = 0.1
POD_MEM = 128 * 1024 ** 2


def test_fits_when_there_is_room():
    t = fill(make_node(name="t", cpu=16, mem_gb=64, pods=211), 100)
    assert t.fits(POD_CPU, POD_MEM) is True


def test_rejects_when_pod_slots_are_exhausted():
    t = fill(make_node(name="t", pods=46), 46)
    assert t.fits(POD_CPU, POD_MEM) is False


def test_rejects_when_cpu_requests_would_exceed_allocatable():
    t = fill(make_node(name="t", cpu=4, pods=46), 5)
    t.cpu_req = t.alloc_cpu - 0.05          # almost full
    assert t.fits(0.5, POD_MEM) is False


def test_rejects_when_memory_requests_would_exceed_allocatable():
    t = fill(make_node(name="t", cpu=16, mem_gb=64, pods=211), 10)
    t.mem_req = t.alloc_mem - GB // 2
    assert t.fits(POD_CPU, 2 * GB) is False


def test_rejects_when_current_real_memory_is_over_the_ceiling():
    t = fill(make_node(name="t", cpu=16, mem_gb=64, pods=211, actual_mem_pct=95), 10)
    assert t.fits(POD_CPU, POD_MEM) is False


def test_rejects_a_different_zone_when_zone_is_required():
    """StatefulSet PVs are zone-scoped: a cross-zone move would sit Pending."""
    t = fill(make_node(name="t", cpu=16, mem_gb=64, pods=211, zone="zone-b"), 10)
    assert t.fits(POD_CPU, POD_MEM, zone="zone-a") is False
    assert t.fits(POD_CPU, POD_MEM, zone="zone-b") is True


def test_rejects_when_the_move_would_push_cpu_requests_over_the_ceiling():
    B.CFG["target_after_cpu_max"] = 50
    t = fill(make_node(name="t", cpu=4, pods=46), 5)
    for i in range(4):
        t.add(_pod_cpu(0.55))               # ~55% of 4 cpu
    t.finalize()
    assert t.fits(POD_CPU, POD_MEM) is False


def test_rejects_when_the_move_would_push_physical_memory_over_the_hard_cap():
    B.CFG["phys_after_mem_max"] = 50
    t = fill(make_node(name="t", cpu=16, mem_gb=64, pods=211, actual_mem_pct=49), 10)
    # the physical cap is evaluated on the pod's *actual* usage, not its request
    assert t.fits(POD_CPU, POD_MEM, act=(0.0, 4 * GB)) is False
    assert t.fits(POD_CPU, POD_MEM, act=(0.0, 64 * 1024 ** 2)) is True


def test_percentage_test_can_be_skipped_for_incomparable_node_shapes():
    """
    Regression: a 46-slot source at 87% moving one pod to a 211-slot target at 86.7%
    looks like "no improvement" (87.0 -> 87.2), which blocked the whole pool-pressure
    driver in practice. Callers skip the score comparison for those moves.
    """
    src = fill(make_node(name="s", cpu=4, pods=46), 40)          # 87.0%
    t = fill(make_node(name="t", cpu=16, mem_gb=64, pods=211), 183)   # 86.7%
    src_after = (len(src.pods) - 1) / src.alloc_pods * 100        # 84.8 after the move
    assert src_after < 87.0

    # without the skip, the target is judged "worse than the source would be"
    assert t.fits(POD_CPU, POD_MEM, src_after_score=src_after, single=True) is False
    # with the skip (ceiling checks only) it is accepted
    assert t.fits(POD_CPU, POD_MEM, src_after_score=None, single=True) is True


def _pod_cpu(cpu):
    from conftest import make_pod
    return make_pod("load", cpu=cpu, mem_mb=64, node="t")
