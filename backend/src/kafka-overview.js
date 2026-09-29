import { AssignerProtocol } from 'kafkajs';

function decodeAssignments(member) {
  try {
    const decoded = AssignerProtocol.MemberAssignment.decode(member.memberAssignment);
    return Object.entries(decoded.assignment ?? {}).map(([topic, partitions]) => ({ topic, partitions }));
  } catch {
    return [];
  }
}

function parseOffset(value) {
  if (value === undefined || value === null || value === '' || value === '-1') return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

async function describeGroup(admin, groupId, topic, logEnds) {
  let description;
  try {
    const result = await admin.describeGroups([groupId]);
    description = result.groups?.find((group) => group.groupId === groupId);
  } catch (error) {
    return {
      id: groupId,
      topic,
      state: 'unavailable',
      error: error.message,
      members: [],
      partitions: [],
      lag: null,
      lagKnown: false,
    };
  }

  const members = (description?.members ?? []).map((member) => ({
    memberId: member.memberId,
    clientId: member.clientId,
    clientHost: member.clientHost,
    assignments: decodeAssignments(member),
  }));

  let committedOffsets = [];
  let offsetError = '';
  if (description && description.state !== 'Dead') {
    try {
      const offsets = await admin.fetchOffsets({ groupId, topics: [topic] });
      committedOffsets = offsets.find((item) => item.topic === topic)?.partitions ?? [];
    } catch (error) {
      offsetError = error.message;
    }
  }

  const committedByPartition = new Map(
    committedOffsets.map((item) => [item.partition, parseOffset(item.offset)]),
  );
  const partitions = (logEnds ?? []).map((item) => {
    const committedOffset = committedByPartition.get(item.partition) ?? null;
    const logEndOffset = parseOffset(item.offset);
    const hasOffsets = committedOffset !== null && logEndOffset !== null;
    return {
      partition: item.partition,
      committedOffset,
      logEndOffset,
      lag: hasOffsets ? Math.max(0, logEndOffset - committedOffset) : null,
    };
  });

  const allKnown = partitions.length > 0 && partitions.every((item) => item.lag !== null);
  const hasCommittedOffsets = committedOffsets.some((item) => parseOffset(item.offset) !== null);
  const state = !description || (description.state === 'Empty' && !hasCommittedOffsets)
    ? 'not-started'
    : (description?.state?.toLowerCase() ?? 'unknown');

  return {
    id: groupId,
    topic,
    state,
    members,
    partitions,
    lag: allKnown ? partitions.reduce((total, item) => total + item.lag, 0) : null,
    lagKnown: allKnown,
    offsetError: offsetError || undefined,
  };
}

export async function getKafkaOverview(admin, topicNames, groupIds) {
  const metadata = await admin.fetchTopicMetadata({ topics: topicNames });
  const topics = await Promise.all(topicNames.map(async (name) => {
    const topicMetadata = metadata.topics.find((item) => item.name === name);
    const logEnds = await admin.fetchTopicOffsets(name);
    const partitions = (topicMetadata?.partitions ?? []).map((partition) => ({
      id: partition.partitionId,
      leader: partition.leader,
      replicas: partition.replicas,
      isr: partition.isr,
    }));
    return { name, partitions, partitionCount: partitions.length, logEnds };
  }));

  const groupTopics = [topicNames[0], topicNames[1]];
  const groups = await Promise.all(groupIds.map((id, index) => {
    const topic = groupTopics[index];
    const matchingTopic = topics.find((item) => item.name === topic);
    return describeGroup(admin, id, topic, matchingTopic?.logEnds ?? []);
  }));

  return { topics: topics.map(({ logEnds, ...topic }) => topic), groups };
}

