import {
    SlashCommandBuilder,
    PermissionFlagsBits,
    ChannelType,
    ModalBuilder,
    TextInputBuilder,
    TextInputStyle,
    ActionRowBuilder,
    MessageFlags,
} from 'discord.js';
import cron from 'node-cron';
import { createEmbed, successEmbed, errorEmbed } from '../../utils/embeds.js';
import { logger } from '../../utils/logger.js';
import { handleInteractionError } from '../../utils/errorHandler.js';
import { InteractionHelper } from '../../utils/interactionHelper.js';
import {
    isValidIanaTimezone,
    listSchedulesForGuild,
    getScheduleByName,
    upsertSchedule,
    deleteSchedule,
    setScheduleEnabled,
    appendPoolItem,
    removePoolItem,
    buildDiscordPayloadFromSchedule,
    pickPoolItem,
} from '../../services/scheduledMessagesService.js';

// ─── validation constants ─────────────────────────────────────────────────────

const NAME_REGEX = /^[\w\-]{1,64}$/;

function validateScheduleName(name) {
    return NAME_REGEX.test(name ?? '');
}

// ─── helpers ──────────────────────────────────────────────────────────────────

function requireDb(interaction, client) {
    const available = client?.db?.isAvailable?.() ?? false;
    if (!available) {
        return interaction.reply({
            embeds: [errorEmbed('Scheduler Unavailable', 'This feature requires PostgreSQL. The database is currently in degraded mode.')],
            flags: MessageFlags.Ephemeral,
        });
    }
    return null;
}

function scheduleListEmbed(rows) {
    if (rows.length === 0) {
        return createEmbed({
            title: '📅 Scheduled Messages',
            description: 'No schedules exist yet. Use `/schedule create` to add one.',
            color: 'info',
        });
    }

    const lines = rows.map(r => {
        const status = r.enabled ? '🟢' : '🔴';
        const mode = r.message_mode === 'pool'
            ? `pool (${r.random_strategy}, ${Array.isArray(r.pool_payload) ? r.pool_payload.length : 0} items)`
            : 'single';
        return `${status} **${r.name}** — <#${r.channel_id}>\n  \`${r.schedule_value}\` · ${r.timezone} · ${mode}`;
    });

    return createEmbed({
        title: `📅 Scheduled Messages (${rows.length})`,
        description: lines.join('\n\n'),
        color: 'primary',
        timestamp: true,
    });
}

// ─── command definition ───────────────────────────────────────────────────────

export default {
    data: new SlashCommandBuilder()
        .setName('schedule')
        .setDescription('Manage scheduled messages')
        .setDMPermission(false)
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)

        // list
        .addSubcommand(sub => sub
            .setName('list')
            .setDescription('List all schedules in this server'))

        // create
        .addSubcommand(sub => sub
            .setName('create')
            .setDescription('Create a new scheduled message')
            .addStringOption(o => o.setName('name').setDescription('Unique name (letters, numbers, - _)').setRequired(true))
            .addChannelOption(o => o.setName('channel').setDescription('Target channel').setRequired(true).addChannelTypes(ChannelType.GuildText, ChannelType.GuildAnnouncement))
            .addStringOption(o => o.setName('cron').setDescription('Cron expression (e.g. 0 9 * * 5 for Fri 9am)').setRequired(true))
            .addStringOption(o => o.setName('mode').setDescription('Message mode').setRequired(true).addChoices(
                { name: 'single', value: 'single' },
                { name: 'pool (random/cycle from a list)', value: 'pool' }
            ))
            .addStringOption(o => o.setName('timezone').setDescription('IANA timezone (default: Africa/Casablanca)').setRequired(false)))

        // edit
        .addSubcommand(sub => sub
            .setName('edit')
            .setDescription('Edit content of an existing schedule')
            .addStringOption(o => o.setName('name').setDescription('Schedule name').setRequired(true)))

        // enable
        .addSubcommand(sub => sub
            .setName('enable')
            .setDescription('Enable a schedule')
            .addStringOption(o => o.setName('name').setDescription('Schedule name').setRequired(true)))

        // disable
        .addSubcommand(sub => sub
            .setName('disable')
            .setDescription('Disable a schedule (no deleting, just pauses it)')
            .addStringOption(o => o.setName('name').setDescription('Schedule name').setRequired(true)))

        // delete
        .addSubcommand(sub => sub
            .setName('delete')
            .setDescription('Permanently delete a schedule')
            .addStringOption(o => o.setName('name').setDescription('Schedule name').setRequired(true)))

        // fire
        .addSubcommand(sub => sub
            .setName('fire')
            .setDescription('Test-fire a schedule immediately')
            .addStringOption(o => o.setName('name').setDescription('Schedule name').setRequired(true)))

        // mentions
        .addSubcommand(sub => sub
            .setName('mentions')
            .setDescription('Configure mention settings for a schedule')
            .addStringOption(o => o.setName('name').setDescription('Schedule name').setRequired(true))
            .addBooleanOption(o => o.setName('everyone').setDescription('@everyone').setRequired(false))
            .addBooleanOption(o => o.setName('here').setDescription('@here').setRequired(false))
            .addStringOption(o => o.setName('roles').setDescription('Comma-separated role IDs (e.g. 123456,789012)').setRequired(false)))

        // pool add
        .addSubcommand(sub => sub
            .setName('pool-add')
            .setDescription('Add an item to a schedule\'s pool')
            .addStringOption(o => o.setName('name').setDescription('Schedule name').setRequired(true)))

        // pool remove
        .addSubcommand(sub => sub
            .setName('pool-remove')
            .setDescription('Remove an item from a schedule\'s pool by index')
            .addStringOption(o => o.setName('name').setDescription('Schedule name').setRequired(true))
            .addIntegerOption(o => o.setName('index').setDescription('0-based item index').setRequired(true).setMinValue(0)))

        // pool strategy
        .addSubcommand(sub => sub
            .setName('pool-strategy')
            .setDescription('Set the pool selection strategy')
            .addStringOption(o => o.setName('name').setDescription('Schedule name').setRequired(true))
            .addStringOption(o => o.setName('strategy').setDescription('Strategy').setRequired(true).addChoices(
                { name: 'random (weighted if items have weight)', value: 'random' },
                { name: 'shuffle cycle (no repeats until full cycle)', value: 'shuffle' }
            ))),

    category: 'Utility',

    async execute(interaction, _config, client) {
        try {
            const dbGuard = requireDb(interaction, client);
            if (dbGuard) return;

            const sub = interaction.options.getSubcommand();
            const guildId = interaction.guildId;
            const userId = interaction.user.id;
            const maxPoolItems = client?.config?.bot?.scheduler?.maxPoolItems ?? 500;

            // ── list ──────────────────────────────────────────────────────────
            if (sub === 'list') {
                const deferOk = await InteractionHelper.safeDefer(interaction, { ephemeral: false });
                if (!deferOk) return;

                const rows = await listSchedulesForGuild(client, guildId);
                return await InteractionHelper.safeEditReply(interaction, {
                    embeds: [scheduleListEmbed(rows)],
                });
            }

            // ── create ────────────────────────────────────────────────────────
            if (sub === 'create') {
                const name = interaction.options.getString('name');
                const channel = interaction.options.getChannel('channel');
                const cronExpr = interaction.options.getString('cron');
                const mode = interaction.options.getString('mode');
                const tz = interaction.options.getString('timezone') ?? 'Africa/Casablanca';

                if (!validateScheduleName(name)) {
                    return interaction.reply({
                        embeds: [errorEmbed('Invalid Name', 'Name must be 1-64 characters: letters, numbers, `-`, `_` only.')],
                        flags: MessageFlags.Ephemeral,
                    });
                }

                if (!cron.validate(cronExpr)) {
                    return interaction.reply({
                        embeds: [errorEmbed('Invalid Cron', `\`${cronExpr}\` is not a valid cron expression.\nExample: \`0 9 * * 5\` = every Friday at 9am.`)],
                        flags: MessageFlags.Ephemeral,
                    });
                }

                if (!isValidIanaTimezone(tz)) {
                    return interaction.reply({
                        embeds: [errorEmbed('Invalid Timezone', `\`${tz}\` is not a recognized IANA timezone.\nExample: \`Africa/Casablanca\`, \`America/New_York\`.`)],
                        flags: MessageFlags.Ephemeral,
                    });
                }

                // Create schedule row (empty content — user edits via /schedule edit)
                const row = await upsertSchedule(client, {
                    guild_id: guildId,
                    name,
                    channel_id: channel.id,
                    message_mode: mode,
                    schedule_mode: 'cron',
                    schedule_value: cronExpr,
                    timezone: tz,
                    enabled: false, // disabled until content is set
                    created_by: userId,
                    updated_by: userId,
                });

                if (!row) {
                    return interaction.reply({
                        embeds: [errorEmbed('Database Error', 'Failed to create schedule. Try again.')],
                        flags: MessageFlags.Ephemeral,
                    });
                }

                // Open content modal immediately so user can set content
                const modal = buildContentModal(name, mode, null);
                return interaction.showModal(modal);
            }

            // ── edit ──────────────────────────────────────────────────────────
            if (sub === 'edit') {
                const name = interaction.options.getString('name');
                const row = await getScheduleByName(client, guildId, name);
                if (!row) {
                    return interaction.reply({
                        embeds: [errorEmbed('Not Found', `No schedule named \`${name}\`.`)],
                        flags: MessageFlags.Ephemeral,
                    });
                }
                const modal = buildContentModal(name, row.message_mode, row);
                return interaction.showModal(modal);
            }

            // ── enable / disable ──────────────────────────────────────────────
            if (sub === 'enable' || sub === 'disable') {
                const deferOk = await InteractionHelper.safeDefer(interaction, { ephemeral: true });
                if (!deferOk) return;

                const name = interaction.options.getString('name');
                const enabling = sub === 'enable';

                const row = await getScheduleByName(client, guildId, name);
                if (!row) {
                    return await InteractionHelper.safeEditReply(interaction, {
                        embeds: [errorEmbed('Not Found', `No schedule named \`${name}\`.)`)],
                    });
                }

                if (enabling && row.message_mode === 'single' && !row.text_content && !row.embed_payload) {
                    return await InteractionHelper.safeEditReply(interaction, {
                        embeds: [errorEmbed('No Content', `Schedule \`${name}\` has no content yet.\nUse \`/schedule edit name:${name}\` to add it first.`)],
                    });
                }

                if (enabling && row.message_mode === 'pool') {
                    const pool = Array.isArray(row.pool_payload) ? row.pool_payload : [];
                    if (pool.length === 0) {
                        return await InteractionHelper.safeEditReply(interaction, {
                            embeds: [errorEmbed('Empty Pool', `Schedule \`${name}\` has no pool items yet.\nUse \`/schedule pool-add name:${name}\` to add items first.`)],
                        });
                    }
                }

                const ok = await setScheduleEnabled(client, guildId, name, enabling, userId);
                const verb = enabling ? 'enabled' : 'disabled';

                return await InteractionHelper.safeEditReply(interaction, {
                    embeds: [ok
                        ? successEmbed(`Schedule ${verb}`, `\`${name}\` is now ${verb}.`)
                        : errorEmbed('Error', 'Failed to update schedule state.')
                    ],
                });
            }

            // ── delete ────────────────────────────────────────────────────────
            if (sub === 'delete') {
                const deferOk = await InteractionHelper.safeDefer(interaction, { ephemeral: true });
                if (!deferOk) return;

                const name = interaction.options.getString('name');
                const ok = await deleteSchedule(client, guildId, name);

                return await InteractionHelper.safeEditReply(interaction, {
                    embeds: [ok
                        ? successEmbed('Schedule Deleted', `\`${name}\` has been permanently deleted.`)
                        : errorEmbed('Not Found', `No schedule named \`${name}\`.`)
                    ],
                });
            }

            // ── fire ──────────────────────────────────────────────────────────
            if (sub === 'fire') {
                const deferOk = await InteractionHelper.safeDefer(interaction, { ephemeral: true });
                if (!deferOk) return;

                const name = interaction.options.getString('name');
                const row = await getScheduleByName(client, guildId, name);

                if (!row) {
                    return await InteractionHelper.safeEditReply(interaction, {
                        embeds: [errorEmbed('Not Found', `No schedule named \`${name}\`.`)],
                    });
                }

                const guild = interaction.guild;
                const channel = guild.channels.cache.get(row.channel_id)
                    ?? await guild.channels.fetch(row.channel_id).catch(() => null);

                if (!channel || !channel.isTextBased()) {
                    return await InteractionHelper.safeEditReply(interaction, {
                        embeds: [errorEmbed('Channel Error', `Could not find or access <#${row.channel_id}>.`)],
                    });
                }

                let payload;
                if (row.message_mode === 'pool') {
                    const poolArr = Array.isArray(row.pool_payload) ? row.pool_payload : [];
                    if (poolArr.length === 0) {
                        return await InteractionHelper.safeEditReply(interaction, {
                            embeds: [errorEmbed('Empty Pool', `Schedule \`${name}\` has no pool items.`)],
                        });
                    }
                    const { item } = pickPoolItem(poolArr, row.random_strategy ?? 'random', row.pool_state, row.last_sent_item_index ?? null);
                    payload = buildDiscordPayloadFromSchedule(row, item);
                } else {
                    payload = buildDiscordPayloadFromSchedule(row, null);
                }

                if (!payload.content && payload.embeds.length === 0) {
                    return await InteractionHelper.safeEditReply(interaction, {
                        embeds: [errorEmbed('No Content', `Schedule \`${name}\` has no content to send.`)],
                    });
                }

                await channel.send({
                    content: payload.content ?? undefined,
                    embeds: payload.embeds.length > 0 ? payload.embeds : undefined,
                    allowedMentions: payload.allowedMentions,
                });

                logger.info('schedule: manual fire triggered', { name, guildId, channelId: row.channel_id, userId });

                return await InteractionHelper.safeEditReply(interaction, {
                    embeds: [successEmbed('Fired', `Schedule \`${name}\` was fired to <#${row.channel_id}>.`)],
                });
            }

            // ── mentions ──────────────────────────────────────────────────────
            if (sub === 'mentions') {
                const deferOk = await InteractionHelper.safeDefer(interaction, { ephemeral: true });
                if (!deferOk) return;

                const name = interaction.options.getString('name');
                const row = await getScheduleByName(client, guildId, name);
                if (!row) {
                    return await InteractionHelper.safeEditReply(interaction, {
                        embeds: [errorEmbed('Not Found', `No schedule named \`${name}\`.`)],
                    });
                }

                const everyone = interaction.options.getBoolean('everyone') ?? row.mention_everyone;
                const here = interaction.options.getBoolean('here') ?? row.mention_here;
                const rolesRaw = interaction.options.getString('roles') ?? null;

                let roleIds = Array.isArray(row.mention_role_ids) ? row.mention_role_ids : [];
                if (rolesRaw !== null) {
                    roleIds = rolesRaw.split(',').map(s => s.trim()).filter(Boolean);
                }

                await upsertSchedule(client, {
                    ...row,
                    mention_everyone: everyone,
                    mention_here: here,
                    mention_role_ids: roleIds,
                    updated_by: userId,
                });

                const parts = [];
                if (everyone) parts.push('@everyone');
                if (here) parts.push('@here');
                if (roleIds.length > 0) parts.push(`roles: ${roleIds.map(id => `<@&${id}>`).join(', ')}`);

                return await InteractionHelper.safeEditReply(interaction, {
                    embeds: [successEmbed('Mentions Updated', parts.length > 0
                        ? `Schedule \`${name}\` will now mention: ${parts.join(', ')}`
                        : `Schedule \`${name}\` will send with no special mentions.`
                    )],
                });
            }

            // ── pool-add ──────────────────────────────────────────────────────
            if (sub === 'pool-add') {
                const name = interaction.options.getString('name');
                const row = await getScheduleByName(client, guildId, name);
                if (!row) {
                    return interaction.reply({
                        embeds: [errorEmbed('Not Found', `No schedule named \`${name}\`.`)],
                        flags: MessageFlags.Ephemeral,
                    });
                }
                if (row.message_mode !== 'pool') {
                    return interaction.reply({
                        embeds: [errorEmbed('Wrong Mode', `Schedule \`${name}\` is in single mode, not pool mode.`)],
                        flags: MessageFlags.Ephemeral,
                    });
                }
                const currentCount = Array.isArray(row.pool_payload) ? row.pool_payload.length : 0;
                if (currentCount >= maxPoolItems) {
                    return interaction.reply({
                        embeds: [errorEmbed('Pool Full', `Max pool size is ${maxPoolItems} items.`)],
                        flags: MessageFlags.Ephemeral,
                    });
                }

                const modal = new ModalBuilder()
                    .setCustomId(`schedule_pool_add:${name}`)
                    .setTitle(`Add Pool Item — ${name}`)
                    .addComponents(
                        new ActionRowBuilder().addComponents(
                            new TextInputBuilder().setCustomId('text_content').setLabel('Text content').setStyle(TextInputStyle.Paragraph).setRequired(false).setMaxLength(2000)
                        ),
                        new ActionRowBuilder().addComponents(
                            new TextInputBuilder().setCustomId('embed_title').setLabel('Embed title (optional)').setStyle(TextInputStyle.Short).setRequired(false).setMaxLength(256)
                        ),
                        new ActionRowBuilder().addComponents(
                            new TextInputBuilder().setCustomId('embed_description').setLabel('Embed description (optional)').setStyle(TextInputStyle.Paragraph).setRequired(false).setMaxLength(4000)
                        ),
                        new ActionRowBuilder().addComponents(
                            new TextInputBuilder().setCustomId('embed_color').setLabel('Embed hex color (e.g. #336699)').setStyle(TextInputStyle.Short).setRequired(false).setMaxLength(7)
                        ),
                        new ActionRowBuilder().addComponents(
                            new TextInputBuilder().setCustomId('weight').setLabel('Weight for random (default: 1)').setStyle(TextInputStyle.Short).setRequired(false).setMaxLength(5)
                        )
                    );
                return interaction.showModal(modal);
            }

            // ── pool-remove ───────────────────────────────────────────────────
            if (sub === 'pool-remove') {
                const deferOk = await InteractionHelper.safeDefer(interaction, { ephemeral: true });
                if (!deferOk) return;

                const name = interaction.options.getString('name');
                const index = interaction.options.getInteger('index');
                const row = await getScheduleByName(client, guildId, name);

                if (!row) {
                    return await InteractionHelper.safeEditReply(interaction, {
                        embeds: [errorEmbed('Not Found', `No schedule named \`${name}\`.`)],
                    });
                }

                const pool = Array.isArray(row.pool_payload) ? row.pool_payload : [];
                if (index >= pool.length) {
                    return await InteractionHelper.safeEditReply(interaction, {
                        embeds: [errorEmbed('Index Out of Range', `Pool has ${pool.length} item(s). Valid indexes: 0–${pool.length - 1}.`)],
                    });
                }

                const newPool = await removePoolItem(client, guildId, name, index, userId);

                return await InteractionHelper.safeEditReply(interaction, {
                    embeds: [newPool !== null
                        ? successEmbed('Item Removed', `Pool item at index ${index} was removed. Pool now has ${newPool.length} item(s).`)
                        : errorEmbed('Error', 'Failed to remove pool item.')
                    ],
                });
            }

            // ── pool-strategy ─────────────────────────────────────────────────
            if (sub === 'pool-strategy') {
                const deferOk = await InteractionHelper.safeDefer(interaction, { ephemeral: true });
                if (!deferOk) return;

                const name = interaction.options.getString('name');
                const strategy = interaction.options.getString('strategy');
                const row = await getScheduleByName(client, guildId, name);

                if (!row) {
                    return await InteractionHelper.safeEditReply(interaction, {
                        embeds: [errorEmbed('Not Found', `No schedule named \`${name}\`.`)],
                    });
                }

                await upsertSchedule(client, { ...row, random_strategy: strategy, updated_by: userId });

                return await InteractionHelper.safeEditReply(interaction, {
                    embeds: [successEmbed('Strategy Updated', `Schedule \`${name}\` now uses **${strategy}** selection.`)],
                });
            }

        } catch (error) {
            logger.error('schedule command: execution failed', {
                error: error.message,
                stack: error.stack,
                userId: interaction.user.id,
                guildId: interaction.guildId,
            });
            await handleInteractionError(interaction, error, {
                commandName: 'schedule',
                source: 'schedule_command',
            });
        }
    },
};

// ─── modal builder for single / pool content editing ─────────────────────────

function buildContentModal(name, mode, existingRow) {
    const isPool = mode === 'pool';
    const modalId = isPool ? `schedule_content_modal:${name}:pool` : `schedule_content_modal:${name}:single`;

    const modal = new ModalBuilder()
        .setCustomId(modalId)
        .setTitle(isPool ? `Set Default Content — ${name}` : `Edit Content — ${name}`);

    modal.addComponents(
        new ActionRowBuilder().addComponents(
            new TextInputBuilder()
                .setCustomId('text_content')
                .setLabel('Text (plain message content)')
                .setStyle(TextInputStyle.Paragraph)
                .setRequired(!isPool)
                .setMaxLength(2000)
                .setValue(existingRow?.text_content ?? '')
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder()
                .setCustomId('embed_title')
                .setLabel('Embed title (optional)')
                .setStyle(TextInputStyle.Short)
                .setRequired(false)
                .setMaxLength(256)
                .setValue(existingRow?.embed_payload?.title ?? '')
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder()
                .setCustomId('embed_description')
                .setLabel('Embed description (optional)')
                .setStyle(TextInputStyle.Paragraph)
                .setRequired(false)
                .setMaxLength(4000)
                .setValue(existingRow?.embed_payload?.description ?? '')
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder()
                .setCustomId('embed_color')
                .setLabel('Embed hex color (e.g. #336699)')
                .setStyle(TextInputStyle.Short)
                .setRequired(false)
                .setMaxLength(7)
                .setValue(existingRow?.embed_payload?.color ?? '')
        )
    );

    return modal;
}
