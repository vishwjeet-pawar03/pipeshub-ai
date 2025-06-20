import type { Metadata, CustomCitation } from 'src/types/chat-bot';
import type { Record, ChatMessageProps, MessageContentProps } from 'src/types/chat-message';

import remarkGfm from 'remark-gfm';
import { Icon } from '@iconify/react';
import ReactMarkdown from 'react-markdown';
import upIcon from '@iconify-icons/mdi/chevron-up';
import eyeIcon from '@iconify-icons/mdi/eye-outline';
import refreshIcon from '@iconify-icons/mdi/refresh';
import loadingIcon from '@iconify-icons/mdi/loading';
import downIcon from '@iconify-icons/mdi/chevron-down';
import robotIcon from '@iconify-icons/mdi/robot-outline';
import rightIcon from '@iconify-icons/mdi/chevron-right';
import accountIcon from '@iconify-icons/mdi/account-outline';
import fileDocIcon from '@iconify-icons/mdi/file-document-outline';
import React, { useRef, useMemo, useState, useCallback } from 'react';

import {
  Box,
  Chip,
  Fade,
  Paper,
  Stack,
  Dialog,
  Button,
  Popper,
  Tooltip,
  Divider,
  Collapse,
  Typography,
  IconButton,
  DialogTitle,
  DialogContent,
  CircularProgress,
  ClickAwayListener,
} from '@mui/material';

import RecordDetails from './record-details';
import MessageFeedback from './message-feedback';
import CitationHoverCard from './citations-hover-card';

const formatTime = (createdAt: Date) => {
  const date = new Date(createdAt);
  return new Intl.DateTimeFormat('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: true,
  }).format(date);
};

const formatDate = (createdAt: Date) => {
  const date = new Date(createdAt);
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);

  if (date.toDateString() === today.toDateString()) {
    return 'Today';
  }
  if (date.toDateString() === yesterday.toDateString()) {
    return 'Yesterday';
  }
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
  }).format(date);
};

function isDocViewable(extension: string) {
  const viewableExtensions = [
    'pdf',
    'xlsx',
    'xls',
    'csv',
    'docx',
    'html',
    'txt',
    'md',
    'ppt',
    'pptx',
  ];
  return viewableExtensions.includes(extension);
}

// Replace the MessageContent component with this updated version

const MessageContent: React.FC<MessageContentProps> = ({
  content,
  citations,
  onRecordClick,
  aggregatedCitations,
  onViewPdf,
}) => {
  // We track the hovered citation by a unique identifier: "lineIndex-partIndex"
  const [hoveredCitationId, setHoveredCitationId] = useState<string | null>(null);
  const [hoveredRecordCitations, setHoveredRecordCitations] = useState<CustomCitation[]>([]);
  const hoverTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Create a map of refs for each citation number
  const citationRefs = useRef<{ [key: string]: HTMLElement | null }>({});

  // Current citation data
  const [hoveredCitation, setHoveredCitation] = useState<CustomCitation | null>(null);

  // Create citation mapping based on chunkIndex - this matches the citation numbers in content
  const citationMapping = useMemo(() => {
    const mapping: { [key: number]: CustomCitation } = {};
    citations.forEach((citation) => {
      if (citation.chunkIndex) {
        mapping[citation.chunkIndex] = citation;
      }
    });
    return mapping;
  }, [citations]);

  const handleCitationInteraction = useCallback(
    (citationRef: string, citationId: string) => {
      if (hoverTimeoutRef.current) {
        clearTimeout(hoverTimeoutRef.current);
      }

      const citationNumber = parseInt(citationRef.replace(/[[\]]/g, ''), 10);
      const citation = citationMapping[citationNumber];

      if (citation) {
        if (citation.metadata?.recordId) {
          const recordCitations = aggregatedCitations[citation.metadata.recordId] || [];
          setHoveredRecordCitations(recordCitations);
        }
        setHoveredCitation(citation);
        setHoveredCitationId(citationId);
      }
    },
    [citationMapping, aggregatedCitations]
  );

  const handleMouseEnter = useCallback(
    (citationRef: string, citationId: string) => {
      handleCitationInteraction(citationRef, citationId);
    },
    [handleCitationInteraction]
  );

  const handleMouseLeave = useCallback(() => {
    hoverTimeoutRef.current = setTimeout(() => {
      setHoveredCitationId(null);
      setHoveredRecordCitations([]);
      setHoveredCitation(null);
    }, 300);
  }, []);

  const handleHoverCardMouseEnter = useCallback(() => {
    if (hoverTimeoutRef.current) {
      clearTimeout(hoverTimeoutRef.current);
    }
  }, []);

  const handleCloseHoverCard = useCallback(() => {
    setHoveredCitationId(null);
    setHoveredRecordCitations([]);
    setHoveredCitation(null);
  }, []);

  const handleClick = useCallback(
    (citationRef: string, citationId: string, event: React.MouseEvent) => {
      event.stopPropagation();

      const citationNumber = parseInt(citationRef.replace(/[[\]]/g, ''), 10);
      const citation = citationMapping[citationNumber];

      if (citation) {
        let recordCitationsForDoc: CustomCitation[] = [];

        if (citation.metadata?.recordId) {
          const recordCitations = aggregatedCitations[citation.metadata.recordId] || [];
          recordCitationsForDoc = recordCitations;
          setHoveredRecordCitations(recordCitations);
        }
        setHoveredCitation(citation);
        setHoveredCitationId(citationId);

        if (citation.metadata?.recordId) {
          try {
            const isExcelOrCSV = ['csv', 'xlsx', 'xls'].includes(citation.metadata?.extension);
            const citationMeta = citation.metadata;
            onViewPdf('', citationMeta, recordCitationsForDoc, isExcelOrCSV);
          } catch (err) {
            console.error('Failed to fetch document:', err);
          }
        }
      }

      if (hoveredCitationId === citationId) {
        handleCloseHoverCard();
      }
    },
    [hoveredCitationId, citationMapping, aggregatedCitations, onViewPdf, handleCloseHoverCard]
  );

  // Process content to show each sentence/point on new lines with citations at the end
  const renderContentWithCitations = useMemo(() => {
    // Split by sentences that end with periods and are followed by numbered points
    const segments = content
      .split(/(?<=\.\s)(?=\d+\.\s)/)
      .filter(segment => segment.trim())
      .map(segment => segment.trim());
    
    return segments.map((segment, segmentIndex) => {
      if (!segment) return null;
      
      // Find all citations in this segment that actually exist in our mapping
      const citationMatches = [...segment.matchAll(/\[(\d+)\]/g)]
        .filter(match => {
          const citationNumber = parseInt(match[1], 10);
          return citationMapping[citationNumber];
        });
      
      // Remove citations from the text for clean display
      const cleanText = segment.replace(/\[\d+\]/g, '').trim();
      
      // If no clean text, skip this segment
      if (!cleanText) return null;
      
      return (
        <Box 
          key={segmentIndex} 
          sx={{ 
            mb: 1,
            display: 'block',
          }}
        >
          <ReactMarkdown
            remarkPlugins={[remarkGfm]}
            components={{
              p: ({ node, ...props }) => (
                <span {...props} style={{ display: 'inline' }}>
                  {props.children}
                  {/* Add citations right after the text content */}
                  {citationMatches.map((match, citIndex) => {
                    const citationNumber = parseInt(match[1], 10);
                    const citation = citationMapping[citationNumber];
                    const citationId = `${segmentIndex}-citation-${citIndex}`;
                    
                    return (
                      <Box
                        key={citationId}
                        component="span"
                        onMouseEnter={() => handleMouseEnter(`[${citationNumber}]`, citationId)}
                        onClick={(e) => handleClick(`[${citationNumber}]`, citationId, e)}
                        onMouseLeave={handleMouseLeave}
                        sx={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          ml: 0.5,
                          cursor: 'pointer',
                          position: 'relative',
                          verticalAlign: 'baseline',
                          '&:hover': {
                            '& .citation-number': {
                              transform: 'scale(1.15) translateY(-1px)',
                              bgcolor: 'primary.main',
                              color: 'white',
                              boxShadow: '0 3px 8px rgba(25, 118, 210, 0.3)',
                            },
                          },
                        }}
                      >
                        <Box
                          component="span"
                          className={`citation-number citation-number-${citationId}`}
                          ref={(el: any) => {
                            citationRefs.current[citationId] = el;
                          }}
                          sx={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            width: '18px',
                            height: '18px',
                            borderRadius: '50%',
                            bgcolor: 'rgba(25, 118, 210, 0.08)',
                            color: 'primary.main',
                            fontSize: '0.65rem',
                            fontWeight: 600,
                            transition: 'all 0.3s cubic-bezier(0.34, 1.56, 0.64, 1)',
                            textDecoration: 'none',
                            boxShadow: '0 1px 3px rgba(0,0,0,0.06)',
                            border: '1px solid',
                            borderColor: 'rgba(25, 118, 210, 0.12)',
                          }}
                        >
                          {citationNumber}
                        </Box>
                      </Box>
                    );
                  })}
                </span>
              ),
            }}
          >
            {cleanText}
          </ReactMarkdown>
        </Box>
      );
    });
  }, [content, citationMapping, handleMouseEnter, handleClick, handleMouseLeave]);

  return (
    <Box sx={{ position: 'relative' }}>
      <Typography
        component="div"
        sx={{
          fontSize: '0.90rem',
          lineHeight: 1.5,
          letterSpacing: '0.01em',
          wordBreak: 'break-word',
          color: 'text.primary',
          fontWeight: 400,
          '& code': {
            backgroundColor: 'rgba(0, 0, 0, 0.04)',
            padding: '0.2em 0.4em',
            borderRadius: '4px',
            fontFamily: 'monospace',
            fontSize: '0.9em',
          },
          '& pre': {
            backgroundColor: 'rgba(0, 0, 0, 0.04)',
            padding: 1.5,
            borderRadius: '4px',
            fontFamily: 'monospace',
            fontSize: '0.85em',
            overflow: 'auto',
            my: 1.5,
            '& code': {
              backgroundColor: 'transparent',
              padding: 0,
            },
          },
          '& strong': {
            fontWeight: 600,
            color: 'text.primary',
          },
          '& a': {
            color: 'primary.main',
            textDecoration: 'none',
            borderBottom: '1px dotted',
            borderColor: 'primary.light',
            transition: 'all 0.2s ease',
            '&:hover': {
              color: 'primary.dark',
              borderColor: 'primary.main',
            },
          },
          '& h1': {
            fontSize: '1.4rem',
            fontWeight: 600,
            marginTop: 2,
            marginBottom: 1.5,
          },
          '& h2': {
            fontSize: '1.2rem',
            fontWeight: 600,
            marginTop: 2,
            marginBottom: 1.5,
          },
          '& h3': {
            fontSize: '1.1rem',
            fontWeight: 600,
            marginTop: 2,
            marginBottom: 1,
          },
          '& h4, & h5, & h6': {
            fontSize: '1rem',
            fontWeight: 600,
            marginTop: 1.5,
            marginBottom: 1,
          },
          '& ul, & ol': {
            paddingLeft: 2.5,
            marginBottom: 1.5,
            marginTop: 0.5,
          },
          '& li': {
            marginBottom: 0.75,
          },
          '& blockquote': {
            borderLeft: '4px solid',
            borderColor: 'divider',
            paddingLeft: 2,
            margin: '1em 0',
            color: 'text.secondary',
            fontStyle: 'italic',
          },
          '& table': {
            borderCollapse: 'collapse',
            width: '100%',
            marginBottom: 2,
          },
          '& th, & td': {
            border: '1px solid',
            borderColor: 'divider',
            padding: '8px 12px',
            textAlign: 'left',
          },
          '& th': {
            backgroundColor: 'rgba(0, 0, 0, 0.02)',
            fontWeight: 600,
          },
        }}
      >
        {renderContentWithCitations}
      </Typography>

      {/* Popper for Citation Hover Card */}
      {hoveredCitationId && hoveredCitation && (
        <Popper
          open={Boolean(hoveredCitationId)}
          anchorEl={citationRefs.current[hoveredCitationId]}
          placement="bottom-start"
          modifiers={[
            {
              name: 'offset',
              options: {
                offset: [0, 8],
              },
            },
            {
              name: 'flip',
              enabled: true,
              options: {
                altBoundary: true,
                rootBoundary: 'viewport',
                padding: 8,
              },
            },
            {
              name: 'preventOverflow',
              enabled: true,
              options: {
                altAxis: true,
                altBoundary: true,
                boundary: 'viewport',
                padding: 16,
              },
            },
          ]}
          sx={{
            zIndex: 9999,
            maxWidth: '95vw',
            width: '380px',
            position: 'fixed',
            pointerEvents: 'none',
          }}
        >
          <ClickAwayListener onClickAway={handleCloseHoverCard}>
            <Box
              onMouseEnter={handleHoverCardMouseEnter}
              onMouseLeave={handleMouseLeave}
              sx={{
                pointerEvents: 'auto',
              }}
            >
              <CitationHoverCard
                citation={hoveredCitation}
                isVisible={Boolean(hoveredCitationId)}
                onRecordClick={(record) => {
                  handleCloseHoverCard();
                  onRecordClick(record);
                }}
                onClose={handleCloseHoverCard}
                aggregatedCitations={hoveredRecordCitations}
                onViewPdf={onViewPdf}
              />
            </Box>
          </ClickAwayListener>
        </Popper>
      )}
    </Box>
  );
};

const ChatMessage = ({
  message,
  isExpanded,
  onToggleCitations,
  index,
  onRegenerate,
  onFeedbackSubmit,
  conversationId,
  isRegenerating,
  showRegenerate,
  onViewPdf,
}: ChatMessageProps) => {
  const [selectedRecord, setSelectedRecord] = useState<Record | null>(null);
  const [isRecordDialogOpen, setRecordDialogOpen] = useState<boolean>(false);
  const aggregatedCitations = useMemo(() => {
    if (!message.citations) return {};

    return message.citations.reduce<{ [key: string]: CustomCitation[] }>((acc, citation) => {
      const recordId = citation.metadata?.recordId;
      if (!recordId) return acc;

      if (!acc[recordId]) {
        acc[recordId] = [];
      }
      acc[recordId].push(citation);
      return acc;
    }, {});
  }, [message.citations]);

  const handleOpenRecordDetails = (record: Record) => {
    const recordCitations = aggregatedCitations[record.recordId] || [];
    setSelectedRecord({ ...record, citations: recordCitations });
    setRecordDialogOpen(true);
  };

  const handleCloseRecordDetails = () => {
    setRecordDialogOpen(false);
    setSelectedRecord(null);
  };

  const handleViewPdf = async (
    url: string,
    citationMeta: Metadata,
    citations: CustomCitation[],
    isExcelFile?: boolean,
    buffer?: ArrayBuffer
  ): Promise<void> =>
    new Promise<void>((resolve) => {
      onViewPdf(url, citationMeta, citations, isExcelFile, buffer);
      resolve();
    });

  const handleViewCitations = async (recordId: string): Promise<void> =>
    new Promise<void>((resolve) => {
      const recordCitations = aggregatedCitations[recordId] || [];
      if (recordCitations.length > 0) {
        const citationMeta = recordCitations[0].metadata;
        onViewPdf('', citationMeta, recordCitations, false);
        resolve();
      }
    });

  return (
    <Box sx={{ mb: 3, width: '100%', position: 'relative' }}>
      {/* Message Metadata */}
      <Box
        sx={{
          mb: 1,
          display: 'flex',
          justifyContent: message.type === 'user' ? 'flex-end' : 'flex-start',
          px: 1.5,
          opacity: isRegenerating ? 0.5 : 1,
          transition: 'opacity 0.2s ease-in-out',
        }}
      >
        <Stack
          direction="row"
          spacing={1}
          alignItems="center"
          sx={{
            px: 1,
            py: 0.25,
            borderRadius: '8px',
            backgroundColor: 'rgba(0, 0, 0, 0.02)',
          }}
        >
          <Icon
            icon={message.type === 'user' ? accountIcon : robotIcon}
            width={14}
            height={14}
            color={message.type === 'user' ? '#1976d2' : '#2e7d32'}
          />
          <Typography
            variant="caption"
            sx={{
              color: 'text.secondary',
              fontSize: '0.65rem',
              fontWeight: 500,
            }}
          >
            {formatDate(message.createdAt)} • {formatTime(message.createdAt)}
          </Typography>
          {message.type === 'bot' && message.confidence && (
            <Tooltip title="Confidence score" placement="top">
              <Chip
                label={message.confidence}
                size="small"
                sx={{
                  height: '20px',
                  fontSize: '0.60rem',
                  fontWeight: 600,
                  backgroundColor:
                    message.confidence === 'Very High'
                      ? 'rgba(46, 125, 50, 0.08)'
                      : 'rgba(237, 108, 2, 0.08)',
                  color: message.confidence === 'Very High' ? '#2e7d32' : '#ed6c02',
                }}
              />
            </Tooltip>
          )}
        </Stack>
      </Box>

      {/* Message Content */}
      <Box sx={{ position: 'relative' }}>
        <Paper
          elevation={1}
          sx={{
            width: '100%',
            maxWidth: '80%',
            p: 2,
            ml: message.type === 'user' ? 'auto' : 0,
            bgcolor: message.type === 'user' ? 'rgba(25, 118, 210, 0.02)' : 'background.paper',
            color: 'text.primary',
            borderRadius: '12px',
            border: '1.5px solid',
            borderColor:
              message.type === 'user' ? 'rgba(25, 118, 210, 0.08)' : 'rgba(0, 0, 0, 0.05)',
            position: 'relative',
            transition: 'all 0.2s ease-in-out',
            opacity: isRegenerating ? 0.5 : 1,
            filter: isRegenerating ? 'blur(0.5px)' : 'none',
            '&:hover': {
              borderColor:
                message.type === 'user' ? 'rgba(25, 118, 210, 0.15)' : 'rgba(0, 0, 0, 0.1)',
              boxShadow: '0 2px 10px rgba(0, 0, 0, 0.03)',
            },
          }}
        >
          {/* Message Content with Citation Hover */}
          {message.type === 'bot' ? (
            <MessageContent
              content={message.content}
              citations={message.citations || []}
              onRecordClick={handleOpenRecordDetails}
              aggregatedCitations={aggregatedCitations}
              onViewPdf={handleViewPdf}
            />
          ) : (
            <Box
              sx={{
                fontSize: '0.9rem',
                lineHeight: 1.6,
                letterSpacing: '0.005em',
                wordBreak: 'break-word',
                '& p': { mt: 0, mb: 1.5 },
                '& h1': { fontSize: '1.4rem', fontWeight: 600, my: 1.5 },
                '& h2': { fontSize: '1.2rem', fontWeight: 600, my: 1.5 },
                '& h3': { fontSize: '1.1rem', fontWeight: 600, my: 1.5 },
                '& h4': { fontSize: '1rem', fontWeight: 600, my: 1.5 },
                '& h5, & h6': { fontSize: '0.9rem', fontWeight: 600, my: 1.5 },
                '& ul, & ol': { pl: 2.5, mb: 1.5, mt: 0 },
                '& li': { mb: 0.75 },
                '& blockquote': {
                  pl: 1.5,
                  ml: 0,
                  borderLeft: '4px solid',
                  borderColor: 'divider',
                  color: 'text.secondary',
                  fontStyle: 'italic',
                  my: 1.5,
                },
                '& code': {
                  backgroundColor: 'rgba(0, 0, 0, 0.04)',
                  padding: '0.2em 0.4em',
                  borderRadius: '4px',
                  fontFamily: 'monospace',
                  fontSize: '0.85em',
                },
                '& pre': {
                  backgroundColor: 'rgba(0, 0, 0, 0.04)',
                  padding: 2,
                  borderRadius: '4px',
                  overflow: 'auto',
                  my: 1.5,
                  '& code': {
                    backgroundColor: 'transparent',
                    padding: 0,
                    borderRadius: 0,
                    fontFamily: 'monospace',
                    fontSize: '0.85em',
                  },
                },
                '& a': {
                  color: 'primary.main',
                  textDecoration: 'none',
                  borderBottom: '1px dotted',
                  borderColor: 'primary.light',
                  transition: 'all 0.2s ease',
                  '&:hover': {
                    color: 'primary.dark',
                    borderColor: 'primary.main',
                  },
                },
                '& img': {
                  maxWidth: '100%',
                  borderRadius: 1,
                },
                '& table': {
                  borderCollapse: 'collapse',
                  width: '100%',
                  mb: 1.5,
                },
                '& th, & td': {
                  border: '1px solid',
                  borderColor: 'divider',
                  padding: '8px 12px',
                  textAlign: 'left',
                },
                '& th': {
                  backgroundColor: 'rgba(0, 0, 0, 0.02)',
                  fontWeight: 600,
                },
              }}
            >
              <ReactMarkdown
              // If you've added remark-gfm:
              // remarkPlugins={[remarkGfm]}
              >
                {message.content}
              </ReactMarkdown>
            </Box>
          )}
          {/* Citations Section */}
          {message.citations && message.citations?.length > 0 && (
            <Box sx={{ mt: 2 }}>
              <Tooltip title={isExpanded ? 'Hide Citations' : 'Show Citations'}>
                <Button
                  variant="text"
                  size="small"
                  onClick={() => onToggleCitations(index)}
                  startIcon={
                    <Icon icon={isExpanded ? downIcon : rightIcon} width={16} height={16} />
                  }
                  sx={{
                    color: 'primary.main',
                    textTransform: 'none',
                    fontWeight: 500,
                    fontSize: '0.7rem',
                    '&:hover': {
                      backgroundColor: 'rgba(25, 118, 210, 0.05)',
                    },
                  }}
                >
                  {message.citations.length} {message.citations.length === 1 ? 'Source' : 'Sources'}
                </Button>
              </Tooltip>

              <Collapse in={isExpanded}>
                <Box sx={{ mt: 2 }}>
                  {message.citations.map((citation, cidx) => (
                    <Paper
                      key={cidx}
                      elevation={0}
                      sx={{
                        p: 2,
                        mb: 2,
                        bgcolor: 'rgba(0, 0, 0, 0.01)',
                        borderRadius: '6px',
                        border: '1px solid',
                        borderColor: 'rgba(0, 0, 0, 0.06)',
                        transition: 'all 0.15s ease-in-out',
                        '&:hover': {
                          backgroundColor: 'rgba(0, 0, 0, 0.02)',
                          borderColor: 'rgba(0, 0, 0, 0.08)',
                        },
                      }}
                    >
                      <Box
                        sx={{
                          pl: 2,
                          borderLeft: '2px solid',
                          borderColor: 'primary.main',
                        }}
                      >
                        <Typography
                          sx={{
                            fontSize: '0.875rem',
                            lineHeight: 1.6,
                            color: 'text.secondary',
                            fontStyle: 'italic',
                            mb: 2,
                          }}
                        >
                          {citation.content}
                        </Typography>

                        {citation.metadata?.recordId && (
                          <Box
                            sx={{
                              display: 'flex',
                              justifyContent: 'flex-end',
                              gap: 1,
                            }}
                          >
                            {isDocViewable(citation.metadata.extension) && (
                              <Button
                                size="small"
                                variant="text"
                                startIcon={<Icon icon={eyeIcon} width={14} height={14} />}
                                onClick={() => handleViewCitations(citation.metadata?.recordId)}
                                sx={{
                                  textTransform: 'none',
                                  fontSize: '0.75rem',
                                  fontWeight: 500,
                                  color: 'primary.main',
                                  py: 0.5,
                                  px: 1,
                                  minWidth: 0,
                                  '&:hover': {
                                    backgroundColor: 'rgba(25, 118, 210, 0.04)',
                                  },
                                }}
                              >
                                View Citations
                              </Button>
                            )}

                            <Button
                              size="small"
                              variant="text"
                              startIcon={<Icon icon={fileDocIcon} width={14} height={14} />}
                              onClick={() => {
                                if (citation.metadata?.recordId) {
                                  const record: Record = {
                                    ...citation.metadata,
                                    citations: [],
                                  };
                                  handleOpenRecordDetails(record);
                                }
                              }}
                              sx={{
                                textTransform: 'none',
                                fontSize: '0.75rem',
                                fontWeight: 500,
                                color: 'text.secondary',
                                py: 0.5,
                                px: 1,
                                minWidth: 0,
                                '&:hover': {
                                  backgroundColor: 'rgba(0, 0, 0, 0.04)',
                                },
                              }}
                            >
                              Details
                            </Button>
                          </Box>
                        )}
                      </Box>
                    </Paper>
                  ))}
                </Box>
              </Collapse>

              {isExpanded && (
                <Tooltip title={isExpanded ? 'Hide Citations' : 'Show Citations'}>
                  <Button
                    variant="text"
                    size="small"
                    onClick={() => onToggleCitations(index)}
                    startIcon={
                      <Icon icon={isExpanded ? upIcon : rightIcon} width={16} height={16} />
                    }
                    sx={{
                      color: 'primary.main',
                      textTransform: 'none',
                      fontWeight: 500,
                      fontSize: '0.7rem',
                      '&:hover': {
                        backgroundColor: 'rgba(25, 118, 210, 0.05)',
                      },
                    }}
                  >
                    Hide citations
                  </Button>
                </Tooltip>
              )}
            </Box>
          )}
          {/* Message Controls */}
          {message.type === 'bot' && (
            <>
              <Divider sx={{ my: 1 }} />
              <Stack direction="row" spacing={1} alignItems="center">
                {showRegenerate && (
                  <>
                    <Tooltip title="Regenerate response">
                      <IconButton
                        onClick={() => onRegenerate(message.id)}
                        size="small"
                        disabled={isRegenerating}
                        sx={{
                          color: 'text.secondary',
                          '&:hover': {
                            color: 'primary.main',
                            backgroundColor: 'rgba(25, 118, 210, 0.05)',
                          },
                        }}
                      >
                        <Icon
                          icon={isRegenerating ? loadingIcon : refreshIcon}
                          width={16}
                          height={16}
                          className={isRegenerating ? 'spin' : ''}
                        />
                      </IconButton>
                    </Tooltip>
                    <MessageFeedback
                      messageId={message.id}
                      conversationId={conversationId}
                      onFeedbackSubmit={onFeedbackSubmit}
                    />
                  </>
                )}
              </Stack>
            </>
          )}
        </Paper>
      </Box>

      {/* Record Details Dialog */}
      <Dialog
        open={isRecordDialogOpen}
        onClose={handleCloseRecordDetails}
        maxWidth="md"
        fullWidth
        PaperProps={{
          elevation: 1,
          sx: {
            borderRadius: '12px',
            boxShadow: '0 4px 20px rgba(0, 0, 0, 0.04)',
          },
        }}
      >
        <DialogTitle
          sx={{
            fontSize: '1rem',
            fontWeight: 500,
            py: 2,
            px: 2.5,
            borderBottom: '1px solid',
            borderColor: 'divider',
          }}
        >
          Record Details
        </DialogTitle>
        <DialogContent
          sx={{
            p: 2.5,
            // ...scrollableContainerStyle
          }}
        >
          {selectedRecord && (
            <RecordDetails
              recordId={selectedRecord.recordId}
              citations={selectedRecord.citations}
            />
          )}
        </DialogContent>
      </Dialog>
      {isRegenerating && (
        <Fade in>
          <Box
            sx={{
              position: 'absolute',
              top: '50%',
              left: message.type === 'user' ? 'auto' : '50%',
              right: message.type === 'user' ? '50%' : 'auto',
              transform: 'translate(-50%, -50%)',
              zIndex: 1,
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              gap: 1,
            }}
          >
            <CircularProgress size={24} />
            <Typography
              variant="caption"
              sx={{
                color: 'text.secondary',
                fontWeight: 500,
                bgcolor: 'background.paper',
                px: 2,
                py: 0.5,
                borderRadius: 1,
                boxShadow: '0 2px 12px rgba(0, 0, 0, 0.03)',
              }}
            >
              Regenerating...
            </Typography>
          </Box>
        </Fade>
      )}
    </Box>
  );
};

export default ChatMessage;
